module EasyNetQ.ProcessManager.Transport.InProcess

open System
open EasyNetQ.ProcessManager

type private ISubscriber =
    abstract CallBack : obj -> unit
    abstract SubscribedType : Type
    abstract Binding : string

type private Subscriber<'a> =
    {
        SubscriptionId : string
        Binding : string
        CallBack : Action<'a>
    }
    interface ISubscriber with
        member x.CallBack o =
            o |> unbox<'a> |> x.CallBack.Invoke
        member __.SubscribedType =
            typeof<'a>
        member x.Binding =
            x.Binding

type private BusControlMessage =
    | Publish of obj * Type * DateTime * string option
    | Subscribe of ISubscriber

let private partCompare (matched, seenHash) (topicPart, bindingPart) =
    if seenHash then
        true, true
    elif not matched then
        false, false
    else
        match bindingPart with
        | "#" -> true, true
        | "*" -> true, false
        | p when p = topicPart -> true, false
        | _ -> false, false

let private splitAndZip (topic : string) (binding : string) =
    let topicParts = topic.Split('.')
    let bindingParts = binding.Split('.')
    if bindingParts.[bindingParts.Length - 1] = "#" then
        let hashes = Seq.unfold (fun () -> Some ("#", ())) ()
        Seq.zip topicParts (Seq.concat [bindingParts |> Array.toSeq;hashes])
        |> Some
    else
        if bindingParts.Length = topicParts.Length then
            Some (Seq.zip topicParts bindingParts)
        else
            None

let private topicCompare topicOpt binding =
    match binding with
    | "#" ->
        true
    | _ ->
        match topicOpt with
        | Some topic ->
            match splitAndZip topic binding with
            | None -> false
            | Some zipped ->
                zipped
                |> Seq.fold partCompare (true, false)
                |> fst
        | None -> false

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
                |> List.filter (fun x -> topicCompare s x.Binding)
                |> List.iter (fun x -> x.CallBack o)
            return! loop subscribers agent
    }

/// Memory based bus implementation designed for testing
type InProcessPMBus () =
    let agent = MailboxProcessor.Start(loop [])
    do agent.Error.Add raise
    interface IPMBus with
        member __.Publish<'a when 'a : not struct> (message : 'a, expire : TimeSpan) =
            agent.Post (Publish (box message, typeof<'a>, DateTime.UtcNow + expire, None))
        member __.Publish<'a when 'a : not struct> (message : 'a, expire : TimeSpan, topic : string) =
            agent.Post (Publish (box message, typeof<'a>, DateTime.UtcNow + expire, Some topic))
        member __.Subscribe<'a when 'a : not struct> (subscriptionId : string, action : Action<'a>) =
            agent.Post (Subscribe ({ SubscriptionId = subscriptionId; Binding = "#"; CallBack = action }))
        member __.Subscribe<'a when 'a : not struct> (subscriptionId : string, topic : string, action: Action<'a>): unit = 
            agent.Post (Subscribe ({ SubscriptionId = subscriptionId; Binding = topic; CallBack = action }))
    interface IDisposable with
        member __.Dispose () = ()
