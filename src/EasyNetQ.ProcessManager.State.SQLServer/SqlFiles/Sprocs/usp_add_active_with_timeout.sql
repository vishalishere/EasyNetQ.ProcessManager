SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

IF EXISTS (SELECT * FROM sysobjects WHERE type = 'P' AND name = 'usp_add_active_with_timeout')
	BEGIN
		PRINT 'Dropping Procedure usp_add_active_with_timeout'
		DROP  Procedure  usp_add_active_with_timeout
	END

GO

PRINT 'Creating Procedure usp_add_active_with_timeout'
GO

CREATE Procedure usp_add_active_with_timeout
	@cid nvarchar(4000),
	@wid uniqueidentifier,
	@type nvarchar(442),
	@next nvarchar(4000),
	@timeOutMs int,	
	@timeOutNext nvarchar(4000)
AS

SET NOCOUNT ON
insert into Active (CorrelationId, WorkflowId, Type, StepName, TimeOut, TimeOutStepName) values (@cid, @wid, @type, @next, DATEADD(ms, @timeOutMs, SYSUTCDATETIME()), @timeOutNext)
SET NOCOUNT OFF
GO
