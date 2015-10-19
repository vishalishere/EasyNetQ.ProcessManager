open System
open Messenger.Messages.Render
open Messenger.Messages.Store
open Messenger.Messages.Email
open Nessos.Argu
open EasyNetQ

type Process1Config =
    | [<Mandatory>] Rabbit_Connection of string
    | [<Mandatory>] Sql_Connection of string
    with 
        interface IArgParserTemplate with
            member ec.Usage =
                match ec with
                | Rabbit_Connection _ -> "specify host name of rabbitmq server"
                | Sql_Connection _ -> "sql server connection string"

let parser = ArgumentParser.Create<Process1Config>()

let template = """Hello {{ name }}!"""

let model = System.Collections.Generic.Dictionary<string, obj>() 
model.Add("name", "bob")

open EasyNetQ.ProcessManager
open EasyNetQ.ProcessManager.State.SqlServer
open Nessos.FsPickler

module Workflow =
    let storeModel model =
        let request = { StoreModel.CorrelationId = Guid.NewGuid(); Model = model }
        Out.Empty
        |> Out.send request (TimeSpan.FromMinutes 4.0)
        |> Out.expect<ModelStored> (request.CorrelationId.ToString()) "StoreTemplate" (TimeSpan.FromMinutes 5.0) (Some "TimeOut")

    let start () =
        storeModel model

    let storeTemplate template (modelStored : ModelStored) (state : IState) =
        let request = { StoreTemplate.CorrelationId  = modelStored.CorrelationId; Name = "mytemplate"; Template = template }
        state.GetOrAdd<int> modelStored.ModelId |> ignore
        Out.Empty
        |> Out.send request (TimeSpan.FromMinutes 4.0)
        |> Out.expect<TemplateStored> (request.CorrelationId.ToString()) "RenderEmail" (TimeSpan.FromMinutes 5.0) (Some "TimeOut")

    let renderEmail (templateStored : TemplateStored) (state : IState) =
        match state.Get<int> () with
        | Some i ->
            let request = { RequestRender.CorrelationId = templateStored.CorrelationId; TemplateId = templateStored.TemplateId; ModelId = i }
            Out.Empty
            |> Out.send request (TimeSpan.FromMinutes 4.0)
            |> Out.expect<RenderComplete> (request.CorrelationId.ToString()) "SendEmail" (TimeSpan.FromMinutes 5.0) (Some "TimeOut")
        | None ->
            failwith "Invalid workflow state, no model in state for email rendering."

    let sendEmail (renderComplete : RenderComplete) (state : IState) =
        let request = 
            match state.Get<int> () with
            | Some i ->
                { SendEmail.CorrelationId = renderComplete.CorrelationId; Content = renderComplete.Content; EmailAddress = sprintf "%d@example.com" i }
            | None ->
                { SendEmail.CorrelationId = renderComplete.CorrelationId; Content = renderComplete.Content; EmailAddress = "me@example.com" }
        Out.Empty
        |> Out.send request (TimeSpan.FromMinutes 4.0)
        |> Out.expect<EmailSent> (request.CorrelationId.ToString()) "EmailSent" (TimeSpan.FromMinutes 5.0) (Some "TimeOut")

    let emailSent (emailSent : EmailSent) (state : IState) =
        printfn "%A" emailSent
        Out.End

    let configure template (pm : ProcessManager) =
        pm.AddProcessor((fun (ms : ModelStored) -> ms.CorrelationId.ToString()), ["StoreTemplate", fun ms state -> storeTemplate template ms state])
        pm.AddProcessor((fun (ts : TemplateStored) -> ts.CorrelationId.ToString()), ["RenderEmail", fun ts state -> renderEmail ts state])
        pm.AddProcessor((fun (rc : RenderComplete) -> rc.CorrelationId.ToString()), ["SendEmail", fun rc state -> sendEmail rc state])
        pm.AddProcessor((fun (es : EmailSent) -> es.CorrelationId.ToString()), ["EmailSent", fun es state -> emailSent es state])
        pm.AddProcessor((fun (timeOut : TimeOutMessage) -> timeOut.CorrelationId.ToString()), ["TimeOut",fun timeOut _ -> printfn "%A" timeOut; Out.End])

type Serializer() =
    let p = Json.FsPickler.CreateJsonSerializer(omitHeader = true)
    interface ISerializer with
        member __.CanSerialize t =
            FsPickler.IsSerializableType t
        member __.CanSerialize<'a> () =
            FsPickler.IsSerializableType<'a> ()
        member __.Serialize<'a> value =
            p.PickleToString<'a> value
        member __.Deserialize<'a> str =
            p.UnPickleOfString<'a> str

[<EntryPoint>]
let main argv = 
    let config = parser.Parse()
    use bus = RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>, fun x -> x.Register<IEasyNetQLogger>(fun _ -> Loggers.ConsoleLogger() :> IEasyNetQLogger) |> ignore)
    let activeStore = SqlActiveStore(config.GetResult <@ Sql_Connection @>)
    let stateStore = SqlStateStore(config.GetResult <@ Sql_Connection @>, Serializer())
    let ipmBus = EasyNetQPMBus(bus)

    // configure subscribers
    let pm = ProcessManager(ipmBus, "Process", activeStore, stateStore)
    Workflow.configure template pm |> ignore

    // start workflow
    pm.Start(fun _ -> Workflow.start ())

    // start workflow a lot, in parallel
    [1..10]
    |> Seq.map (fun _ -> Async.Sleep 500 |> Async.RunSynchronously)
    |> Seq.iter (fun _ -> pm.Start(fun _ -> Workflow.start()))

    Console.ReadLine() |> ignore
    0 