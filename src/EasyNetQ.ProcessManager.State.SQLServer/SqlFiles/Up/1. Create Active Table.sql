/****** Object:  Table [dbo].[Active]    Script Date: 09/24/2015 15:52:30 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Active](
	[ActiveId] [bigint] IDENTITY(1,1) NOT NULL,
	[CorrelationId] [nvarchar](4000) NOT NULL,
	[WorkflowId] [uniqueidentifier] NOT NULL,
	[Type] [nvarchar](442) NOT NULL,
	[StepName] [nvarchar](4000) NOT NULL,
	[TimeOut] [datetime] NOT NULL,
	[TimeOutStepName] [nvarchar](4000) NULL,
 CONSTRAINT [PK_Active] PRIMARY KEY CLUSTERED 
(
	[ActiveId] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO


