open System
open EasyNetQ
open Nessos.Argu
open Messenger.Messages.Store

type StoreConfig =
    | [<Mandatory>] Rabbit_Connection of string
    with
        interface IArgParserTemplate with
            member ec.Usage =
                match ec with
                | Rabbit_Connection _ -> "specify host name of rabbitmq server"

let parser = ArgumentParser.Create<StoreConfig>()
let config = parser.Parse()

type Model =
    | Store of StoreModel
    | BroadCast of BroadcastModel

let modelStore (bus : IBus) =
    MailboxProcessor.Start(
        fun agent ->
            let rec loop i m =
                async {
                    let! msg = agent.Receive()
                    match msg with
                    | Model.Store sm ->
                        bus.Publish { ModelStored.CorrelationId = sm.CorrelationId; ModelId = i }
                        return!
                            Map.add i sm.Model m
                            |> loop (i + 1)
                    | BroadCast bm ->
                        let model = Map.find bm.ModelId m
                        bus.Publish { ModelBroadcasted.CorrelationId = bm.CorrelationId; Model = model; ModelId = bm.ModelId }
                        return! loop i m
                }
            loop 0 Map.empty
    )

type Template =
    | Store of StoreTemplate
    | BroadCast of BroadcastTemplate

let templateStore (bus : IBus) =
    MailboxProcessor.Start(
        fun agent ->
            let rec loop i m =
                async {
                    let! msg = agent.Receive()
                    match msg with
                    | Template.Store sm ->
                        bus.Publish { TemplateStored.CorrelationId = sm.CorrelationId; TemplateId = i }
                        return!
                            Map.add i (sm.Name, sm.Template) m
                            |> loop (i + 1)
                    | BroadCast bm ->
                        let name, template = Map.find bm.TemplateId m
                        bus.Publish { TemplateBroadcasted.CorrelationId = bm.CorrelationId; Template = template; TemplateId = bm.TemplateId; Name = name }
                        return! loop i m
                }
            loop 0 Map.empty
    )

[<EntryPoint>]
let main argv = 
    use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>)
    let subscriptionId = "Messenger.Store"
    let mStore = modelStore bus
    let tStore = templateStore bus
    bus.Subscribe(subscriptionId, fun st -> st |> Template.Store |> tStore.Post) |> ignore
    bus.Subscribe(subscriptionId, fun sm -> sm |> Model.Store |> mStore.Post) |> ignore
    bus.Subscribe(subscriptionId, fun bt -> bt |> Template.BroadCast |> tStore.Post) |> ignore
    bus.Subscribe(subscriptionId, fun bm -> bm |> Model.BroadCast |> mStore.Post) |> ignore
    printfn "Store ready"
    Console.ReadLine() |> ignore
    0
