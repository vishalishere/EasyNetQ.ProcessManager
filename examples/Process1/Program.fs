open System
open Messenger.Messages.Render
open Messenger.Messages.Store
open Messenger.Messages.Email
open Nessos.Argu
open EasyNetQ

type Process1Config =
    | [<Mandatory>] Rabbit_Connection of string
    with 
        interface IArgParserTemplate with
            member ec.Usage =
                match ec with
                | Rabbit_Connection _ -> "specify host name of rabbitmq server"

let parser = ArgumentParser.Create<Process1Config>()

let template = """Hello {{ name }}!"""

let model = System.Collections.Generic.Dictionary<string, obj>() 
model.Add("name", "bob")

[<EntryPoint>]
let main argv = 
    let config = parser.Parse()
    use bus = RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>)

    // Subscribers
    bus.Subscribe<RenderComplete> ("Process", fun r -> printfn "%A" r) |> ignore
    bus.Subscribe<ModelStored> ("Process", fun ms -> printfn "%A" ms) |> ignore
    bus.Subscribe<TemplateStored> ("Process", fun ts -> printfn "%A" ts) |> ignore
    bus.Subscribe<EmailSent> ("Process", fun es -> printfn "%A" es) |> ignore

    // Senders
    bus.Publish { StoreModel.CorrelationId = Guid.NewGuid(); Model = model }
    bus.Publish { StoreTemplate.CorrelationId = Guid.NewGuid(); Name = "mytemplate"; Template = template }
    bus.Publish { RequestRender.CorrelationId = Guid.NewGuid(); TemplateId = 1; ModelId = 4 }
    bus.Publish { SendEmail.CorrelationId = Guid.NewGuid(); Content = "Hello world!"; EmailAddress = "me@example.com" }
    Console.ReadLine() |> ignore
    0 