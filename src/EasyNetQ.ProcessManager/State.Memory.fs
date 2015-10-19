module EasyNetQ.ProcessManager.State.Memory

open System
open System.Collections.Concurrent
open EasyNetQ.ProcessManager

type private MemoryStateMissing() =
    inherit Exception()

type MemoryState () =
    let store = ConcurrentDictionary<Type, obj>()
    interface IState with
        member __.AddOrUpdate<'a> value (transform : Func<'a, 'a>) =
            store.AddOrUpdate(typeof<'a>, box value, fun _ v -> v |> unbox<'a> |> transform.Invoke |> box) |> unbox<'a>
        member __.GetOrAdd<'a> value =
            store.GetOrAdd(typeof<'a>, box value) |> unbox<'a>
        member __.Get<'a> () =
            match store.TryGetValue typeof<'a> with
            | true, v -> Some (unbox<'a> v)
            | false, _ -> None
        member x.TryUpdate<'a> (update : Func<'a, 'a>, v : byref<'a>) =
            let state = x :> IState
            if store.ContainsKey(typeof<'a>) then
                let newValue = state.AddOrUpdate<'a> Unchecked.defaultof<'a> update
                v <- newValue
                true
            else
                false

// In memory state store - designed only to be used for testing.
type MemoryStateStore () =
    let store = ConcurrentDictionary<Guid, IState>()
    interface IStateStore with
        member __.Add (WorkflowId g) state =
            store.AddOrUpdate(g, state, fun s _ -> failwithf "Attempted to add duplicate state to statestore with key %s" <| s.ToString()) |> ignore
        member __.Remove (WorkflowId g) =
            store.TryRemove(g) |> ignore
        member __.Get (WorkflowId g) =
            store.GetOrAdd(g, fun g -> failwithf "Attempted to retrieve non-existent state from statestore with key %s" <| g.ToString())
        member __.Create (_) =
            MemoryState () :> IState

type private MemoryStoreMessages =
    | Add of Type * Active
    | Remove of Type * CorrelationId * StepName * WorkflowId
    | Conts of Type * CorrelationId * AsyncReplyChannel<(StepName * WorkflowId) list>
    | WorkflowActive of WorkflowId * AsyncReplyChannel<bool>
    | CancelWorkflow of WorkflowId
    | ProcessElapsed of (Elapsed -> unit) * AsyncReplyChannel<WorkflowId seq>

// In memory active store - designed only to be used for testing.
type MemoryActiveStore () =
    let agent =
        MailboxProcessor.Start(
            fun mb ->
                let rec loop actives =
                    async {
                        let! msg = mb.Receive()
                        match msg with
                        | Add (t, a) ->
                            return! loop <| (t, a, DateTime.UtcNow + a.TimeOut)::actives
                        | WorkflowActive (wid, rc) ->
                            actives
                            |> List.exists (fun (_, a, _) -> a.WorkflowId = wid)
                            |> rc.Reply
                            return! loop actives
                        | Remove (t, cid, next, wid) ->
                            return!
                                actives
                                |> List.filter (
                                    fun (t', a, _) ->
                                        (t' = t 
                                        && cid = a.CorrelationId
                                        && next = a.NextStep
                                        && wid = a.WorkflowId)
                                        |> not)
                                |> loop
                        | CancelWorkflow wid ->
                            return!
                                actives
                                |> List.filter (fun (_, a, _) -> a.WorkflowId <> wid)
                                |> loop
                        | Conts (t, cid, rc) ->
                            actives
                            |> List.filter (fun (t', a, to') -> t = t' && a.CorrelationId = cid && to' > DateTime.UtcNow)
                            |> List.map (fun (_, a, _) -> a.NextStep, a.WorkflowId)
                            |> rc.Reply
                            return! loop actives
                        | ProcessElapsed (f, rc) ->
                            let timedOut, stillActive =
                                actives
                                |> List.partition (fun (_, _, to') -> to' <= DateTime.UtcNow)
                            timedOut
                            |> List.iter (fun (_, a, _) ->
                                                    match a.TimeOutNextStep with
                                                    | Some n -> f { TimedOut = a.NextStep; NextStep = n; WorkflowId = a.WorkflowId }
                                                    | None -> ())
                            timedOut
                            |> List.map (fun (_, { WorkflowId = w }, _) -> w)
                            |> Seq.distinct
                            |> rc.Reply
                            return! loop stillActive
                    }
                loop []
            )
    interface IActiveStore with
        member __.Add<'a> (active : Active) = 
            Add (typeof<'a>, active) |> agent.Post
        member __.Remove<'a> (correlationId, n, wid) =
            Remove (typeof<'a>, correlationId, n, wid) |> agent.Post
        member __.Continuations<'a> correlationId =
            agent.PostAndReply(fun rc -> Conts (typeof<'a>, correlationId, rc))
        member __.WorkflowActive wid =
            agent.PostAndReply(fun rc -> WorkflowActive (wid, rc))
        member __.CancelWorkflow wid =
            CancelWorkflow wid |> agent.Post
        member __.ProcessElapsed f =
            agent.PostAndReply(fun rc -> ProcessElapsed (f, rc))

