open System.Net.Mail
open Messenger.Email
open Nessos.Argu
open EasyNetQ

type EmailConfig =
    | [<Mandatory>] Rabbit_Connection of string
    | [<Mandatory>] Smtp_Server of string
    with 
        interface IArgParserTemplate with
            member ec.Usage =
                match ec with
                | Rabbit_Connection _ -> "specify host name of rabbitmq server"
                | Smtp_Server _ -> "specify the smtp server to send messages with"

let parser = ArgumentParser.Create<EmailConfig>()

[<EntryPoint>]
let main args =
    let config = parser.Parse ()
//    use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>, fun x -> x.Register<IEasyNetQLogger>(fun _ -> Loggers.ConsoleLogger() :> IEasyNetQLogger) |> ignore)
    use bus = EasyNetQ.RabbitHutch.CreateBus(config.GetResult <@ Rabbit_Connection @>)
    let action request =
        send (new SmtpClient(config.GetResult <@ Smtp_Server @>)) request
        |> bus.Publish
    printfn "Email ready"
    bus.Subscribe ("Messenger.Email", fun r -> action r) |> ignore
    System.Console.ReadLine() |> ignore
    0