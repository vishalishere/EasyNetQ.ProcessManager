module EasyNetQ.ProcessManager.Transport.InProcess

open System
open EasyNetQ.ProcessManager

type private ISubscriber =
    abstract CallBack : obj -> unit
    abstract SubscribedType : Type

type private Subscriber<'a> =
    {
        SubscriptionId : string
        Topic : string
        CallBack : Action<'a>
    }
    interface ISubscriber with
        member x.CallBack o =
            o |> unbox<'a> |> x.CallBack.Invoke
        member x.SubscribedType =
            typeof<'a>

type private BusControlMessage =
    | Publish of obj * Type * DateTime * string option
    | Subscribe of ISubscriber

let rec private loop subscribers (agent : MailboxProcessor<BusControlMessage>) =
    async {
        let! msg = agent.Receive()
        match msg with
        | Subscribe s ->
            return! loop (s::subscribers) agent
        | Publish (o, t, ts, s) ->
            if ts > DateTime.UtcNow then
                subscribers
                |> List.filter (fun x -> t = x.SubscribedType)
                |> List.iter (fun x -> x.CallBack o)
            return! loop subscribers agent
    }

/// Memory based bus implementation designed for testing
type InProcessPMBus () =
    let agent = MailboxProcessor.Start(loop [])
    interface IPMBus with
        member __.Publish<'a when 'a : not struct> (message : 'a, expire : TimeSpan) =
            agent.Post (Publish (box message, typeof<'a>, DateTime.UtcNow + expire, None))
        member __.Publish<'a when 'a : not struct> (message : 'a, expire : TimeSpan, topic : string) =
            agent.Post (Publish (box message, typeof<'a>, DateTime.UtcNow + expire, Some topic))
        member __.Subscribe<'a when 'a : not struct> (subscriptionId : string, action : Action<'a>) =
            agent.Post (Subscribe ({ SubscriptionId = subscriptionId; Topic = "#"; CallBack = action }))
        member __.Subscribe<'a when 'a : not struct> (subscriptionId : string, topic : string, action: Action<'a>): unit = 
            agent.Post (Subscribe ({ SubscriptionId = subscriptionId; Topic = topic; CallBack = action }))
    interface IDisposable with
        member __.Dispose () = ()

