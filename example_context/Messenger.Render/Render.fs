open System
open System.Collections.Generic
open Messenger.Messages.Render
open Messenger.Messages.Store
open Nessos.Argu
open Nessos.FsPickler
open DotLiquid
open EasyNetQ.ProcessManager
open EasyNetQ.ProcessManager.State.Memory

type RenderConfig =
    | [<Mandatory>] Rabbit_Connection of string
    with 
        interface IArgParserTemplate with
            member ec.Usage =
                match ec with
                | Rabbit_Connection _ -> "specify host name of rabbitmq server"

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
    |> Out.send { BroadcastTemplate.CorrelationId = correlationId; TemplateId = request.TemplateId } (TimeSpan.FromMinutes 5.0)
    |> Out.expect<TemplateBroadcasted> (correlationId.ToString()) "TemplateBroadcasted" (TimeSpan.FromMinutes 5.0) None
    |> Out.send { BroadcastModel.CorrelationId = correlationId; ModelId = request.ModelId } (TimeSpan.FromMinutes 5.0)
    |> Out.expect<ModelBroadcasted> (correlationId.ToString()) "ModelBroadcasted" (TimeSpan.FromMinutes 5.0) None

let renderIfReady guid ri =
    match ri with
    | { Model = Some model; Template = Some template } ->
        let template = Template.Parse template
        let hash = Hash.FromDictionary model
        let content = template.Render(hash) 
        Out.End
        |> Out.send { Content = content; CorrelationId = guid } (TimeSpan.FromMinutes 5.0)
    | _ -> Out.Ignore

let templateBroadcasted (tb : TemplateBroadcasted) (state : IState) =
    let updated = state.AddOrUpdate { Model = None; Template = Some tb.Template} (Func<_,_>(fun r -> { r with RenderInfo.Template = Some tb.Template }))
    renderIfReady tb.CorrelationId updated

let modelBroadcasted (mb : ModelBroadcasted) (state : IState) =
    let updated = state.AddOrUpdate { Model = Some mb.Model; Template = None } (Func<_,_>(fun r -> { r with Model = Some mb.Model }))
    renderIfReady mb.CorrelationId updated

[<EntryPoint>]
let main args =
    let config = parser.Parse ()
    //use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>, fun x -> x.Register<IEasyNetQLogger>(fun _ -> Loggers.ConsoleLogger() :> IEasyNetQLogger) |> ignore)
    use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>)
    let activeStore = MemoryActiveStore()
    let stateStore = MemoryStateStore()

    use pmbus = new EasyNetQPMBus(bus)
    use pm = new ProcessManager(pmbus, "Messenger.Render", activeStore, stateStore)
    pm.AddProcessor<TemplateBroadcasted>((fun (tb : TemplateBroadcasted) -> tb.CorrelationId.ToString()), ["TemplateBroadcasted", fun tb state -> templateBroadcasted tb state])
    pm.AddProcessor<ModelBroadcasted>((fun (mb : ModelBroadcasted) -> mb.CorrelationId.ToString()), ["ModelBroadcasted", fun mb state -> modelBroadcasted mb state])
    pm.StartFromSubcribe<RequestRender>(fun r state -> requestTemplateAndModel r state)
    printfn "Render ready"
    System.Console.ReadLine() |> ignore
    0