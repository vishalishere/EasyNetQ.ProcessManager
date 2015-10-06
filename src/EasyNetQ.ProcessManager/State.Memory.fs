module EasyNetQ.ProcessManager.State.Memory

open System
open System.Collections.Concurrent
open EasyNetQ.ProcessManager

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

// In memory active store - designed only to be used for testing.
type MemoryActiveStore () =
    let store = ConcurrentDictionary<string * Type, (StepName * WorkflowId * DateTime * StepName option) list>()
    member __.Add<'message> (CorrelationId correlationId) nextStep stateId timeOut timeOutNextStep =
        store.AddOrUpdate((correlationId, typeof<'message>), [(nextStep, stateId, DateTime.Now + timeOut, timeOutNextStep)], fun _ conts -> (nextStep, stateId, DateTime.Now + timeOut, timeOutNextStep)::conts)
        |> ignore
    member __.Remove<'a> (CorrelationId correlationId) =
        store.TryRemove((correlationId, typeof<'a>)) |> ignore
    member __.Continuations<'a> (CorrelationId correlationId) =
        match store.TryGetValue((correlationId, typeof<'a>)) with
        | true, conts ->
            conts
            |> List.filter (fun (_, _, time, _) -> time < DateTime.Now)
        | _ -> []
    member __.WorkflowActive wid =
        store.Values
        |> Seq.concat
        |> Seq.exists (fun (_, activeId, _, _) -> activeId = wid)
    member __.CancelWorkflow wid =
        let (delete, update) =
            store
            |> Seq.fold
                (fun (delete, update) kv -> 
                    if kv.Value |> List.forall (fun (_, activeId, _, _) -> activeId = wid) then 
                        (kv.Key::delete), update 
                    elif kv.Value |> List.exists (fun (_, activeId, _, _) -> activeId = wid) then 
                        delete,(kv.Key, (kv.Value |> List.filter (fun (_, activeId, _, _) -> activeId = wid)))::update 
                    else delete, update)
                ([], [])
        delete |> List.map store.TryRemove |> ignore
        update |> List.map (fun (key, value) -> store.AddOrUpdate(key, value, fun _ _ -> value)) |> ignore
    member __.ProcessElapsed f =
        store.Values
        |> Seq.concat
        |> Seq.filter (fun (_, _, time, _) -> time < DateTime.Now)
        |> Seq.filter (fun (_, _, _, next) -> match next with Some _ -> true | None -> false)
        |> Seq.map (
            fun (timedOut, wid, _, next) -> 
                match next with
                | Some n ->
                    do f {
                                TimedOut = timedOut
                                NextStep = next.Value
                                WorkflowId = wid 
                            }
                    wid
                | None -> wid)
        |> Seq.distinct
    interface IActiveStore with
        member x.Add<'a> (active : Active) = 
            x.Add<'a> active.CorrelationId active.NextStep active.WorkflowId active.TimeOut active.TimeOutNextStep
        member x.Remove<'a> correlationId = x.Remove<'a> correlationId
        member x.Continuations<'a> correlationId =
            x.Continuations<'a> correlationId
            |> List.map (fun (a, b, _, _) -> a, b)
        member x.WorkflowActive wid = x.WorkflowActive wid
        member x.CancelWorkflow wid = x.CancelWorkflow wid
        member x.ProcessElapsed f = x.ProcessElapsed f

