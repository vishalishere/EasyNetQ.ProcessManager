namespace EasyNetQ.ProcessManager.Transport.EasyNetQLegacy

open System
open EasyNetQ
open EasyNetQ.FluentConfiguration
open EasyNetQ.ProcessManager

[<AutoOpen>]
module private ExpiringPublish =

    let publish<'a when 'a : not struct> (bus : IBus) (conv : IConventions) (ped : Producer.IPublishExchangeDeclareStrategy) message (expire : TimeSpan) topic =
        let messageType = typeof<'a>
        let exchangeName = conv.ExchangeNamingConvention.Invoke messageType
        let props = MessageProperties(Expiration = (expire.TotalMilliseconds |> int |> string))
        let enqMessage = Message<'a>(message)
        enqMessage.SetProperties props
        let exchange = ped.DeclareExchange(bus.Advanced, exchangeName, Topology.ExchangeType.Topic)
        let finalTopic =
            match topic with
            | Some t -> t
            | None -> "#"
        bus.Advanced.Publish(exchange, finalTopic, false, false, enqMessage)

/// Legacy implementation of IPMBus, using an older EasyNetQ. 
type EasyNetQPMBus (bus : IBus) =
    let ped = bus.Advanced.Container.Resolve<Producer.IPublishExchangeDeclareStrategy>()
    let conventions = bus.Advanced.Container.Resolve<IConventions>()
    interface IPMBus with
        member __.Publish<'a when 'a : not struct> (message : 'a, expire) =
            publish<'a> bus conventions ped message expire None
        member __.Publish<'a when 'a : not struct> (message : 'a, expire, topic) =
            publish<'a> bus conventions ped message expire (Some topic)
        member __.Subscribe<'a when 'a : not struct> (subscriptionId, action) =
            bus.Subscribe<'a>(subscriptionId, action) |> ignore
        member __.Subscribe<'a when 'a : not struct> (subscriptionId, topic, action: Action<'a>): unit = 
            bus.Subscribe<'a>(subscriptionId, action, Action<ISubscriptionConfiguration>(fun x -> x.WithTopic topic |> ignore)) |> ignore
    interface IDisposable with
        member __.Dispose() =
            bus.Dispose()