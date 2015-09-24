open System
open System.Collections.Generic
open Messenger.Messages.Render
open Messenger.Messages.Store
open Nessos.Argu
open Nessos.FsPickler
open DotLiquid
open EasyNetQ.ProcessManager
open EasyNetQ.ProcessManager.State.SqlServer

type RenderConfig =
    | [<Mandatory>] Rabbit_Connection of string
    | [<Mandatory>] Sql_Connection of string
    with 
        interface IArgParserTemplate with
            member ec.Usage =
                match ec with
                | Rabbit_Connection _ -> "specify host name of rabbitmq server"
                | Sql_Connection _ -> "sql server connection string"

type RenderInfo =
    {
        Model : IDictionary<string, obj> option
        Template : string option
    }

let parser = ArgumentParser.Create<RenderConfig>()

let requestTemplateAndModel (request : RequestRender) (state : IState)  =
    let correlationId = request.CorrelationId
    state.AddOrUpdate<RenderInfo> { Model = None; Template = None } (Func<_,_>(fun _ -> { Model = None; Template = None })) |> ignore
    Out.Empty
    |> Out.AddR { BroadcastTemplate.CorrelationId = correlationId; TemplateId = request.TemplateId } (TimeSpan.FromMinutes 5.0)
    |> Out.AddC<TemplateBroadcasted> (correlationId.ToString()) "TemplateBroadcasted" (TimeSpan.FromMinutes 5.0) None
    |> Out.AddR { BroadcastModel.CorrelationId = correlationId; ModelId = request.ModelId } (TimeSpan.FromMinutes 5.0)
    |> Out.AddC<ModelBroadcasted> (correlationId.ToString()) "ModelBroadcasted" (TimeSpan.FromMinutes 5.0) None

let renderIfReady guid ri =
    match ri with
    | { Model = Some model; Template = Some template } ->
        let template = Template.Parse template
        let hash = Hash.FromDictionary model
        let content = template.Render(hash) 
        Out.End
        |> Out.AddR { Content = content; CorrelationId = guid } (TimeSpan.FromMinutes 5.0)
    | _ -> Out.Ignore

let templateBroadcasted (tb : TemplateBroadcasted) (state : IState) =
    let updated = state.AddOrUpdate { Model = None; Template = Some tb.Template} (Func<_,_>(fun r -> { r with RenderInfo.Template = Some tb.Template }))
    renderIfReady tb.CorrelationId updated

let modelBroadcasted (mb : ModelBroadcasted) (state : IState) =
    let updated = state.AddOrUpdate { Model = Some mb.Model; Template = None } (Func<_,_>(fun r -> { r with Model = Some mb.Model }))
    renderIfReady mb.CorrelationId updated

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
let main args =
    let config = parser.Parse ()
    //use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>, fun x -> x.Register<IEasyNetQLogger>(fun _ -> Loggers.ConsoleLogger() :> IEasyNetQLogger) |> ignore)
    use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>)
    let activeStore = SqlActiveStore(config.GetResult <@ Sql_Connection @>)
    let stateStore = SqlStateStore(config.GetResult <@ Sql_Connection @>, Serializer())

    let pm = ProcessManager(EasyNetQPMBus(bus), "Messenger.Render", activeStore, stateStore)
    pm.AddProcessor<TemplateBroadcasted>((fun (tb : TemplateBroadcasted) -> tb.CorrelationId.ToString()), ["TemplateBroadcasted", fun tb state -> templateBroadcasted tb state])
    pm.AddProcessor<ModelBroadcasted>((fun (mb : ModelBroadcasted) -> mb.CorrelationId.ToString()), ["ModelBroadcasted", fun mb state -> modelBroadcasted mb state])
    pm.StartFromSubcribe<RequestRender>(fun r state -> requestTemplateAndModel r state)
    printfn "Render ready"
    System.Console.ReadLine() |> ignore
    0