[<AutoOpen>]
module private EasyNetQ.ProcessManager.State.SqlServer.Sql
open System
open System.Data
open System.Data.SqlClient
open EasyNetQ.ProcessManager

let getWorkflowLock (conn : SqlConnection) trans (wid : string) =
    use app_lock = conn.CreateCommand()
    app_lock.CommandType <- CommandType.StoredProcedure
    app_lock.CommandText <- "sp_getapplock"
    app_lock.Parameters.Add (SqlParameter("Resource", wid)) |> ignore
    app_lock.Parameters.Add (SqlParameter("LockMode", "Exclusive")) |> ignore
    app_lock.Parameters.Add (SqlParameter("LockTimeout", 60 * 1000)) |> ignore
    let rc = SqlParameter()
    rc.Direction <- ParameterDirection.ReturnValue
    app_lock.Parameters.Add (rc) |> ignore
    app_lock.Transaction <- trans
    app_lock.ExecuteNonQuery() |> ignore
    match rc.Value :?> int with
    | i when i >= 0 -> ()
    | _ -> failwith "Workflow lock unavailable"

let safeContext<'a> connString (serializer : ISerializer) (WorkflowId wid) type' (action : SqlConnection -> SqlTransaction -> 'a) =
    match type' with
    | Some t ->
        if not <| serializer.CanSerialize t then
            failwith "Provided serializer cannot serialize type %A" t
    | None -> ()
    use conn = new SqlConnection(connString)
    conn.Open()
    use trans = conn.BeginTransaction(IsolationLevel.ReadUncommitted)
    try
        getWorkflowLock conn trans <| wid.ToString()
        let result = action conn trans
        trans.Commit()
        result
    with
    | e ->
        trans.Rollback()
        raise e

let getRawState (conn : SqlConnection) trans (wid : Guid) (t : Type) =
    let text = "select State from State where WorkflowId = @wid and Type = @t"
    use retrieve = conn.CreateCommand()
    retrieve.CommandText <- text
    retrieve.CommandType <- CommandType.Text
    retrieve.Parameters.Add (SqlParameter("wid", wid)) |> ignore
    retrieve.Parameters.Add (SqlParameter("t", t.FullName)) |> ignore
    retrieve.Transaction <- trans
    use dataReader = retrieve.ExecuteReader()
    match dataReader.HasRows with
    | false ->
        None
    | true ->
        dataReader.Read() |> ignore
        let result = dataReader.GetString(0) |> Some
        if dataReader.Read() then
            failwith "More than one state stored for workflow %A, type %s - have you changed the State table settings?" wid
        result

let insertRawState (conn : SqlConnection) trans (wid : Guid) (t : Type) (state : string) =
    let text = "insert into State (WorkflowId, Type, State) values (@wid, @t, @state)"
    use insert = conn.CreateCommand()
    insert.CommandText <- text
    insert.CommandType <- CommandType.Text
    insert.Parameters.Add (SqlParameter("wid", wid)) |> ignore
    insert.Parameters.Add (SqlParameter("t", t.FullName)) |> ignore
    insert.Parameters.Add (SqlParameter("state", state)) |> ignore
    insert.Transaction <- trans
    insert.ExecuteNonQuery() |> ignore

let updateRawState (conn : SqlConnection) trans (wid : Guid) (t : Type) (state :string) =
    let text = "update State set State = @state where WorkflowId = @wid and Type = @t"
    use update = conn.CreateCommand()
    update.CommandText <- text
    update.CommandType <- CommandType.Text
    update.Parameters.Add (SqlParameter("wid", wid)) |> ignore
    update.Parameters.Add (SqlParameter("t", t.FullName)) |> ignore
    update.Parameters.Add (SqlParameter("state", state)) |> ignore
    update.Transaction <- trans
    update.ExecuteNonQuery() |> ignore

let deleteWorkflowState (conn : SqlConnection) trans (wid : Guid) =
    let text = "delete from State where WorkflowId = @wid"
    use delete = conn.CreateCommand()
    delete.CommandText <- text
    delete.CommandType <- CommandType.Text
    delete.Parameters.Add (SqlParameter("wid", wid)) |> ignore
    delete.Transaction <- trans
    delete.ExecuteNonQuery() |> ignore

let insertActive (conn : SqlConnection) (cid : string) (wid : Guid) (t : Type) (next : string) (timeOut : TimeSpan) (timeOutNext : string) =
    let text =
        if timeOutNext = null then
            "usp_add_active_no_timeout"
        else
            "usp_add_active_with_timeout"
    use insert = conn.CreateCommand()
    insert.CommandText <- text
    insert.CommandType <- CommandType.StoredProcedure
    insert.Parameters.Add (SqlParameter("cid", cid)) |> ignore
    insert.Parameters.Add (SqlParameter("wid", wid)) |> ignore
    insert.Parameters.Add (SqlParameter("type", t.FullName)) |> ignore
    insert.Parameters.Add (SqlParameter("next", next)) |> ignore
    insert.Parameters.Add (SqlParameter("timeOutMs", timeOut.TotalMilliseconds)) |> ignore
    if timeOutNext <> null then insert.Parameters.Add (SqlParameter("timeOutNext", timeOutNext)) |> ignore
    insert.ExecuteNonQuery() |> ignore

let containsActive (conn : SqlConnection) (cid : string) (t : Type) =
    let text = "select StepName, WorkflowId from Active where CorrelationId = @cid and Type = @t"
    use select = conn.CreateCommand()
    select.CommandText <- text
    select.CommandType <- CommandType.Text
    select.Parameters.Add (SqlParameter("cid", cid)) |> ignore
    select.Parameters.Add (SqlParameter("t", t.FullName)) |> ignore
    let reader = select.ExecuteReader()
    seq {
        while reader.Read() do
            yield (reader.GetString(0), reader.GetGuid(1))
        reader.Dispose()
    }

let deleteActive (conn : SqlConnection) (cid : string) (t : Type) =
    let text = "delete from Active where CorrelationId = @cid and Type = @t"
    use delete = conn.CreateCommand()
    delete.CommandText <- text
    delete.CommandType <- CommandType.Text
    delete.Parameters.Add (SqlParameter("cid", cid)) |> ignore
    delete.Parameters.Add (SqlParameter("t", t.FullName)) |> ignore
    delete.ExecuteNonQuery() |> ignore

let workflowActive (conn : SqlConnection) (wid : Guid) =
    let text = "select WorkflowId from Active where WorkflowId = @wid"
    use select = conn.CreateCommand()
    select.CommandText <- text
    select.CommandType <- CommandType.Text
    select.Parameters.Add (SqlParameter("wid", wid)) |> ignore
    use reader = select.ExecuteReader()
    reader.HasRows

let cancelWorkflow (conn : SqlConnection) (wid : Guid) =
    let text = "delete from Active where WorkflowId = @wid"
    use delete = conn.CreateCommand()
    delete.CommandText <- text
    delete.CommandType <- CommandType.Text
    delete.Parameters.Add (SqlParameter("wid", wid)) |> ignore
    delete.ExecuteNonQuery() |> ignore

type ActiveLock =
    | Error of int
    | Acquired
    | Locked

let getActiveLock (conn : SqlConnection) (trans : SqlTransaction) =
    use app_lock = conn.CreateCommand()
    app_lock.CommandType <- CommandType.StoredProcedure
    app_lock.CommandText <- "sp_getapplock"
    app_lock.Parameters.Add (SqlParameter("Resource", "Processing elapsed")) |> ignore
    app_lock.Parameters.Add (SqlParameter("LockMode", "Exclusive")) |> ignore
    app_lock.Parameters.Add (SqlParameter("LockTimeout", 0)) |> ignore
    let rc = SqlParameter()
    rc.Direction <- ParameterDirection.ReturnValue
    app_lock.Parameters.Add (rc) |> ignore
    app_lock.Transaction <- trans
    app_lock.ExecuteNonQuery() |> ignore
    match rc.Value :?> int with
    | 0 -> Acquired
    | -1 -> Locked
    | i -> Error i

let elapsedActive (conn : SqlConnection) (f : Guid * string * string * int64 -> int64) =
    let trans = conn.BeginTransaction(IsolationLevel.ReadUncommitted)
    match getActiveLock conn trans with
    | Acquired ->
        let selectText = "SELECT TOP 100 WorkflowId, StepName, TimeOutStepName, ActiveId FROM Active WHERE TimeOut < SYSUTCDATETIME()"
        use select = conn.CreateCommand()
        select.CommandText <- selectText
        select.CommandType <- CommandType.Text
        select.Transaction <- trans
        use reader = select.ExecuteReader()
        let (touchedWorkflows, processedActives) =
            seq {
                while reader.Read() do
                    yield (reader.GetGuid(0), reader.GetString(1), reader.GetSqlString(2), reader.GetInt64(3))
                reader.Dispose()
            }
            |> Seq.map (fun el ->
                match el with
                | wid, step, next, i when not next.IsNull ->
                    wid, f (wid, step, next.Value, i)
                | wid, _, _, i -> wid, i)
            |> Seq.toList
            |> List.unzip
            |> fun (wids, is) -> wids |> Seq.distinct |> Seq.toList, is
        match processedActives with
        | [] ->
            trans.Commit()
            []
        | _ ->
            let ids =
                processedActives
                |> List.mapi (fun i _ -> sprintf "@aid%d" i)
            let deleteText = sprintf "DELETE Active WHERE ActiveId IN (%s)" (ids |> String.concat ", ")
            use delete = conn.CreateCommand()
            delete.CommandText <- deleteText
            delete.CommandType <- CommandType.Text
            processedActives
            |> Seq.zip ids
            |> Seq.iter (fun (name, value) -> delete.Parameters.Add(name, SqlDbType.BigInt).Value <- value)
            delete.Transaction <- trans
            delete.ExecuteNonQuery() |> ignore
            trans.Commit()
            touchedWorkflows
    | Locked ->
        trans.Commit()
        []
    | Error i -> failwithf "Acquiring elapsed active lock failed with %d" i
