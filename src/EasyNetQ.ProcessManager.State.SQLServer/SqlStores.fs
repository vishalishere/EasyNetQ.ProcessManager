namespace EasyNetQ.ProcessManager.State.SqlServer

open EasyNetQ.ProcessManager

open System.Data.SqlClient

// SqlServer backed persistent state; requires state table to be deployed.
type SqlState (connString : string, workflowId : WorkflowId, serializer : ISerializer) =
    member private __.wid = match workflowId with WorkflowId w -> w
    interface IState with
        member x.AddOrUpdate<'a> (value : 'a) updateFunc =
            let t = typeof<'a>
            let action value conn trans =
                match getRawState conn trans x.wid t with
                | None ->
                    insertRawState conn trans x.wid t (serializer.Serialize value)
                    value
                | Some old ->
                    let new' =
                        old
                        |> serializer.Deserialize
                        |> updateFunc.Invoke
                    new'
                    |> serializer.Serialize
                    |> updateRawState conn trans x.wid t
                    new'
            safeContext<'a> connString serializer workflowId (Some t) (action value)
        member x.Get<'a> () =
            let t = typeof<'a>
            let action conn trans =
                getRawState conn trans x.wid t
                |> Option.map (serializer.Deserialize)
            safeContext<'a option> connString serializer workflowId (Some t) action
        member x.GetOrAdd<'a> (value : 'a) =
            let t = typeof<'a>
            let action value conn trans =
                match getRawState conn trans x.wid t with
                | None ->
                    insertRawState conn trans x.wid t (serializer.Serialize value)
                    value
                | Some raw ->
                    serializer.Deserialize raw
            safeContext<'a> connString serializer workflowId (Some t) (action value)

// SqlServer backed persistent state store; requires state table to be deployed.
type SqlStateStore (connString : string, serializer : ISerializer) =
    interface IStateStore with
        member __.Add (_ : WorkflowId) (_ : IState) = ()
        member __.Remove w =
            safeContext<unit> connString serializer w None (fun conn trans -> match w with WorkflowId wid -> deleteWorkflowState conn trans wid)
        member __.Get w =
            SqlState (connString, w, serializer) :> IState
        member __.Create w =
            SqlState (connString, w, serializer) :> IState
         
// SqlServer backed persistent active store; requires active table and sprocs to be deployed.
type SqlActiveStore (connString : string) =
    interface IActiveStore with
        member __.Add<'a> { CorrelationId = (CorrelationId cid); WorkflowId = (WorkflowId wid); NextStep = (StepName next); TimeOut = timeOut; TimeOutNextStep = timeOutNext } =
            use conn = new SqlConnection(connString)
            conn.Open()
            match timeOutNext with
            | Some (StepName tnext) ->
                insertActive conn cid wid typeof<'a> next timeOut tnext
            | None ->
                insertActive conn cid wid typeof<'a> next timeOut null
        member __.Continuations<'a> (CorrelationId cid) =
            use conn = new SqlConnection(connString)
            conn.Open()
            containsActive conn cid typeof<'a>
            |> Seq.map (fun (next, wid) -> StepName next, WorkflowId wid)
            |> Seq.toList
        member __.Remove<'a> (CorrelationId cid) =
            use conn = new SqlConnection(connString)
            conn.Open()
            deleteActive conn cid typeof<'a>
        member __.WorkflowActive (WorkflowId wid) =
            use conn = new SqlConnection(connString)
            conn.Open()
            workflowActive conn wid
        member __.CancelWorkflow (WorkflowId wid) =
            use conn = new SqlConnection(connString)
            conn.Open()
            cancelWorkflow conn wid
        member __.ProcessElapsed f =
            use conn = new SqlConnection(connString)
            conn.Open()
            let rawToElapsed (wid : System.Guid, timedOut : string, next : string, aid : int64) =
                let elapsed = { TimedOut = StepName timedOut; NextStep = StepName next; WorkflowId = WorkflowId wid }
                f elapsed
                aid
            elapsedActive conn rawToElapsed
            |> Seq.map WorkflowId