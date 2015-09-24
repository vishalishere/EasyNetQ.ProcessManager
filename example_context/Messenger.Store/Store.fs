open System
open FSharp.Data.Sql
open Nessos.Argu
open Nessos.FsPickler
open Messenger.Messages.Store

type StoreConfig =
    | [<Mandatory>] Rabbit_Connection of string
    | [<Mandatory>] Sql_Connection of string
    with 
        interface IArgParserTemplate with
            member ec.Usage =
                match ec with
                | Rabbit_Connection _ -> "specify host name of rabbitmq server"
                | Sql_Connection _ -> "sql server connection string"

type db = SqlDataProvider<ConnectionString = "Server=(local);Database=Messenger;Trusted_Connection=true">
let parser = ArgumentParser.Create<StoreConfig>()
let config = parser.Parse()
let ctx = db.GetDataContext(config.GetResult <@ Sql_Connection @>)
let json = Json.FsPickler.CreateJsonSerializer(omitHeader = true)

let storeModel (sm : StoreModel) : ModelStored =
    let modelJson = json.PickleToString(sm.Model)
    let row = ctx.``[dbo].[Model]``.Create()
    row.model <- modelJson
    ctx.SubmitUpdates()
    { ModelId = row.model_id; CorrelationId = sm.CorrelationId }

let storeTemplate (st : StoreTemplate) : TemplateStored =
    let row = ctx.``[dbo].[Templates]``.Create()
    row.template_name <- st.Name
    row.template <- st.Template
    ctx.SubmitUpdates()
    { TemplateId = row.template_id; CorrelationId = st.CorrelationId }

let broadcastModel (bm : BroadcastModel) : ModelBroadcasted =
    let modelRow =
        ctx.``[dbo].[Model]``
        |> Seq.where (fun m -> m.model_id = bm.ModelId)
        |> Seq.exactlyOne
    let model =
        json.UnPickleOfString modelRow.model
    { CorrelationId = bm.CorrelationId; ModelId = modelRow.model_id; Model = model }

let broadcastTemplate (bt : BroadcastTemplate) =
    let templateRow =
        ctx.``[dbo].[Templates]``
        |> Seq.where (fun t -> t.template_id = bt.TemplateId)
        |> Seq.exactlyOne
    { CorrelationId = bt.CorrelationId; TemplateId = bt.TemplateId; Template = templateRow.template; Name = templateRow.template_name}

[<EntryPoint>]
let main argv = 
    //use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>, fun x -> x.Register<IEasyNetQLogger>(fun _ -> Loggers.ConsoleLogger() :> IEasyNetQLogger) |> ignore)
    use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>)
    let subscriptionId = "Messenger.Store"
    bus.Subscribe(subscriptionId, fun st -> storeTemplate st |> bus.Publish) |> ignore
    bus.Subscribe(subscriptionId, fun sm -> storeModel sm |> bus.Publish) |> ignore
    bus.Subscribe(subscriptionId, fun bt -> broadcastTemplate bt |> bus.Publish) |> ignore
    bus.Subscribe(subscriptionId, fun bm -> broadcastModel bm |> bus.Publish) |> ignore
    printfn "Store ready"
    Console.ReadLine() |> ignore
    0
