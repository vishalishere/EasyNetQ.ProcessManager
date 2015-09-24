SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

IF EXISTS (SELECT * FROM sysobjects WHERE type = 'P' AND name = 'usp_add_active_no_timeout')
	BEGIN
		PRINT 'Dropping Procedure usp_add_active_no_timeout'
		DROP  Procedure  usp_add_active_no_timeout
	END

GO

PRINT 'Creating Procedure usp_add_active_no_timeout'
GO

CREATE Procedure usp_add_active_no_timeout
	@cid nvarchar(4000),
	@wid uniqueidentifier,
	@type nvarchar(442),
	@next nvarchar(4000),
	@timeOutMs int
AS

SET NOCOUNT ON
insert into Active (CorrelationId, WorkflowId, Type, StepName, TimeOut) values (@cid, @wid, @type, @next, DATEADD(ms, @timeOutMs, SYSUTCDATETIME()))
SET NOCOUNT OFF
GO
