namespace EasyNetQ.ProcessManager

open System
open System.Collections.Generic
open EasyNetQ
open System.Collections.Concurrent
open EasyNetQ.FluentConfiguration

type CorrelationId = CorrelationId of string
type WorkflowId = WorkflowId of Guid
type StepName = StepName of string

type IState =
    abstract member AddOrUpdate<'a> : 'a -> Func<'a, 'a> -> 'a
    abstract member GetOrAdd<'a> : 'a -> 'a
    abstract member Get<'a> : unit -> 'a option

type IStateStore =
    abstract member Add : WorkflowId -> IState -> unit
    abstract member Remove : WorkflowId -> unit
    abstract member Get : WorkflowId -> IState
    abstract member Create : WorkflowId -> IState

type Active =
    {
        CorrelationId : CorrelationId
        NextStep : StepName
        WorkflowId : WorkflowId
        TimeOut : TimeSpan
        TimeOutNextStep : StepName option
    }

type Elapsed =
    {
        TimedOut : StepName
        NextStep : StepName
        WorkflowId : WorkflowId
    }

type IActiveStore =
    abstract member Add<'a> : Active -> unit
    abstract member Remove<'a> : CorrelationId -> unit
    abstract member Continuations<'a> : CorrelationId -> (StepName * WorkflowId) list
    abstract member WorkflowActive : WorkflowId -> bool
    abstract member CancelWorkflow : WorkflowId -> unit
    abstract member ProcessElapsed : (Elapsed -> unit) -> WorkflowId seq

type IPMBus =
    abstract member Publish<'a when 'a : not struct> : ('a * TimeSpan) -> unit
    abstract member Publish<'a when 'a : not struct> : ('a * TimeSpan * string) -> unit
    abstract member Subscribe<'a when 'a : not struct> : string * Action<'a> -> unit
    abstract member Subscribe<'a when 'a : not struct> : string * string * Action<'a> -> unit

[<AutoOpen>]
module private ExpiringPublish =
    let publish<'a when 'a : not struct> (bus : IBus) (dms : IMessageDeliveryModeStrategy) (ped : Producer.IPublishExchangeDeclareStrategy) message (expire : TimeSpan) topic =
        let messageType = typeof<'a>
        let props = MessageProperties(Expiration = (expire.TotalMilliseconds |> int |> string), DeliveryMode = dms.GetDeliveryMode(messageType))
        let enqMessage = Message<'a>(message, props)
        let exchange = ped.DeclareExchange(bus.Advanced, messageType, Topology.ExchangeType.Topic)
        let finalTopic =
            match topic with
            | Some t -> t
            | None -> bus.Advanced.Conventions.TopicNamingConvention.Invoke messageType
        bus.Advanced.Publish(exchange, finalTopic, false, false, enqMessage)

type EasyNetQPMBus (bus : IBus) =
    let dms = bus.Advanced.Container.Resolve<IMessageDeliveryModeStrategy>()
    let ped = bus.Advanced.Container.Resolve<Producer.IPublishExchangeDeclareStrategy>()
    interface IPMBus with
        member __.Publish<'a when 'a : not struct> ((message : 'a, expire)) =
            publish<'a> bus dms ped message expire None
        member __.Publish<'a when 'a : not struct> ((message : 'a, expire, topic)) =
            publish<'a> bus dms ped message expire (Some topic)
        member __.Subscribe<'a when 'a : not struct> (subscriptionId, action) =
            bus.Subscribe<'a>(subscriptionId, action) |> ignore
        member __.Subscribe<'a when 'a : not struct> (subscriptionId, topic, action: Action<'a>): unit = 
            bus.Subscribe<'a>(subscriptionId, action, Action<ISubscriptionConfiguration>(fun x -> x.WithTopic topic |> ignore)) |> ignore

type IRequest =
    abstract member Publish : IPMBus -> unit

type Request<'a when 'a : not struct> =
    | Request of 'a * TimeSpan
    | TopicRequest of 'a * TimeSpan * string
    interface IRequest with
        member x.Publish (bus : IPMBus) =
            match x with
            | Request(request, expiry) ->
                bus.Publish<'a> ((request, expiry))
            | TopicRequest (request, expiry, topic) ->
                bus.Publish<'a> ((request, expiry, topic))

type IContinuation =
    abstract member AddActive : IActiveStore * WorkflowId -> unit

type Continuation<'message when 'message : not struct> (correlationId, handler, timeOut, timeOutHandler) =
    new (correlationId, handler) =
        Continuation(correlationId, handler, TimeSpan.FromDays(1.0), None)
    interface IContinuation with
        member __.AddActive (store : IActiveStore, workflowId) =
            store.Add<'message> {
                CorrelationId = CorrelationId correlationId
                NextStep = StepName handler
                WorkflowId = workflowId
                TimeOut = timeOut
                TimeOutNextStep = Option.map StepName timeOutHandler
            }

type CallBacks =
    | Cancel
    | Conts of IContinuation list

type Out =
    {
        Requests : IRequest list
        Continuations : CallBacks
    }
    static member End =
        { Requests = []; Continuations = Cancel }
    static member Ignore = { Requests = []; Continuations = Conts [] }
    static member Empty = Out.Ignore
    static member AddR r expiry out =
        { out with Requests = Request (r, expiry) :> IRequest::out.Requests }
    static member AddTopicR r expiry topic out =
        { out with Requests = TopicRequest (r, expiry, topic) :> IRequest::out.Requests }
    static member AddC<'response when 'response : not struct> correlationId handler timeOut timeOutHandler x =
        let c = Continuation<'response>(correlationId, handler, timeOut, timeOutHandler)
        match x with
        | { Continuations = Cancel } -> failwith "You cannot add continuations to a workflow that is being cancelled."
        | { Continuations = Conts cs } -> { x with Continuations = Conts <| (c :> IContinuation)::cs }
    member x.AddRequest<'message when 'message : not struct> (m, expiry) = Out.AddR<'message> m expiry x
    member x.AddTopicRequest<'message when 'message : not struct> (m, expiry, topic) = Out.AddTopicR m expiry topic x
    member x.AddCont<'response when 'response : not struct> (correlationId, handler, timeOut, timeOutHandler) =
        Out.AddC<'response> correlationId handler timeOut (Some timeOutHandler) x
    member x.AddCont<'response when 'response : not struct> (correlationId, handler, timeOut) =
        Out.AddC<'response> correlationId handler timeOut None x

type private DispatchMap<'message when 'message : not struct>() = 
    let storage = Dictionary<string, 'message -> IState -> Out>()
    member __.TryGetAction name =
        storage.TryGetValue name
    member __.AddAction(name, action) =
        storage.Add(name, action)
    new(name, action) as this =
        DispatchMap()
        then this.AddAction(name, action)

type private IProcessor =
    abstract member ExtractCorrelationId : obj -> string

type private Processor<'message when 'message : not struct> =
    {
        ExtractCorrelationId : 'message -> string
        DispatchMap : DispatchMap<'message>
    }
    interface IProcessor with
        member x.ExtractCorrelationId (o : obj) =
            x.ExtractCorrelationId (unbox o)

type Mapping<'a when 'a : not struct> =
    {
        Key : string
        Func : Func<'a, IState, Out>
    }

type private SubscriptionManager (bus : IPMBus, subscriptionId, activeStore : IActiveStore, stateStore : IStateStore) =
    let pc = ConcurrentDictionary<Type, obj>()
    member private __.receive<'a when 'a : not struct> (a :  'a) =
        let activateProcessor (p: Processor<'a>) =
            let cid = p.ExtractCorrelationId a |> CorrelationId
            let workflows =
                activeStore.Continuations<'a> cid
                |> List.map (fun (next, workflowId) ->
                    match p.DispatchMap.TryGetAction (match next with StepName name -> name) with
                    | true, f ->
                        let output = f a (stateStore.Get workflowId)
                        match output.Continuations with
                        | Conts cs ->
                            cs |> Seq.iter (fun c -> c.AddActive (activeStore, workflowId))
                        | Cancel ->
                            activeStore.CancelWorkflow workflowId
                            stateStore.Remove workflowId
                        output.Requests
                        |> Seq.iter (fun p -> p.Publish bus)
                        workflowId
                    | false, _ -> failwithf "Active correlation ID %A, but no handler for %A in the subscription manager" cid next )
            cid, (Seq.distinct workflows)
        match pc.TryGetValue typeof<'a> with
        | true, (:? (Processor<'a> []) as ps) ->
            ps
            |> Array.map activateProcessor
            |> Array.map (fun (cid, workflows) -> activeStore.Remove<'a> cid; workflows)
            |> Seq.concat
            |> Seq.iter (fun wid -> if not (activeStore.WorkflowActive wid) then stateStore.Remove wid)
        | _ -> ()
    member private x.AddProcessor<'message when 'message : not struct> (processor : Processor<'message>) =
        if
                pc.AddOrUpdate((typeof<'message>), box [|processor|], fun _ old -> Array.concat [unbox old;[|processor|]] |> box)
                :?> Processor<'message> []
                |> Array.length
                |> (=) 1 then
            bus.Subscribe<'message> (subscriptionId, Action<'message>(fun message -> x.receive message))
    static member Add extractCorrelationId (dispatchMappings : #seq<_>) (sm : SubscriptionManager) =
        let dispatchMap = DispatchMap()
        dispatchMappings
        |> Seq.iter (fun (key, f) -> dispatchMap.AddAction(key, f))
        sm.AddProcessor ({ ExtractCorrelationId = extractCorrelationId; DispatchMap = dispatchMap })
    member x.AddProcessor<'message when 'message : not struct> (extractCorrelationId : Func<'message, string>, dispatchMappings) =
        let mappings =
            dispatchMappings
            |> Seq.map (fun { Key = key; Func = f } -> key, (fun m s -> f.Invoke(m, s)))
        SubscriptionManager.Add (fun m -> extractCorrelationId.Invoke m) mappings x
    member x.AddProcessor<'message when 'message : not struct> (extractCorrelationId : Func<'message, string>, dispatchMapping) =
        x.AddProcessor(extractCorrelationId, [dispatchMapping])
    member private __.StartLogic (output : Out) stateId =
        match output.Continuations with
        | Cancel -> ()
        | Conts c ->
            c |> Seq.iter (fun c -> c.AddActive(activeStore, stateId))
        output.Requests
        |> Seq.iter (fun p -> p.Publish bus)
    member x.StartWorkflow(starter : Func<IState, Out>) =
        let stateId = Guid.NewGuid() |> WorkflowId
        let state = stateStore.Create stateId
        stateStore.Add stateId state
        let output = starter.Invoke state
        x.StartLogic output stateId
    member x.StartFromSubscibe<'message when 'message : not struct>(starter : Func<'message, IState, Out>, ?topic : string) =
        let handler (m : 'message) =
            let stateId = Guid.NewGuid() |> WorkflowId
            let state = stateStore.Create stateId
            stateStore.Add stateId state
            let output = starter.Invoke(m, state)
            x.StartLogic output stateId
        match topic with
        | Some t ->
            bus.Subscribe<'message>(subscriptionId, t, fun m -> handler m)
        | None ->
            bus.Subscribe<'message>(subscriptionId, fun m -> handler m)

type TimeOutMessage =
    {
        CorrelationId : Guid
        TimedOutStep : string
    }

type private TimeManager (bus : IPMBus, activeStore : IActiveStore, stateStore : IStateStore) =
    let rand = Random()
    let publishTimeOut (e : Elapsed) =
        let cid = Guid.NewGuid()
        let a = { CorrelationId = cid.ToString() |> CorrelationId; NextStep = e.NextStep; TimeOut = TimeSpan.FromDays 1.0; TimeOutNextStep = None; WorkflowId = e.WorkflowId }
        activeStore.Add<TimeOutMessage> a
        let (StepName t) = e.TimedOut
        bus.Publish<TimeOutMessage>(({ CorrelationId = cid; TimedOutStep = t }, TimeSpan.FromHours 23.0))

    let rec removeTimedOut () =
        async {
            activeStore.ProcessElapsed publishTimeOut
            |> Seq.filter (activeStore.WorkflowActive >> not)
            |> Seq.iter (fun wid -> stateStore.Remove wid)

            do! Async.Sleep (rand.Next(1000,2000))
            return! removeTimedOut ()
        }
    do removeTimedOut () |> Async.Start
        
type ProcessManager (bus : IPMBus, subscriptionId, activeStore, stateStore) =
    let subscriptionManager = SubscriptionManager(bus, subscriptionId, activeStore, stateStore)
    let _ = TimeManager(bus, activeStore, stateStore)
    member __.AddProcessor<'message when 'message : not struct>(extractCorrelationId, mappings : Mapping<'message> seq) =
        subscriptionManager.AddProcessor<'message>(extractCorrelationId, mappings) |> ignore
    member __.AddProcessor<'message when 'message : not struct>(extractCorrelationId, mapping : Mapping<'message>) =
        subscriptionManager.AddProcessor<'message>(extractCorrelationId, mapping) |> ignore
    member x.AddProcessor<'message when 'message : not struct>(extractCorrelationId : 'message -> string, mappings) =
        let ms =
            mappings
            |> Seq.map (fun (key, f) -> { Key = key; Func = Func<'message, IState, Out> (f) })
        x.AddProcessor<'message>(Func<_,_>(fun m -> extractCorrelationId m), ms)
    member __.Start(startFunc) =
        subscriptionManager.StartWorkflow(startFunc)
    member __.StartFromSubcribe<'startMessage when 'startMessage : not struct>(startFunc) =
        subscriptionManager.StartFromSubscibe<'startMessage>(startFunc)
    member __.StartFromSubcribe<'startMessage when 'startMessage : not struct>(startFunc, topic) =
        subscriptionManager.StartFromSubscibe<'startMessage>(startFunc, topic)

