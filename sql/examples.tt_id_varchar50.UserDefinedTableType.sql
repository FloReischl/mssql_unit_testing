CREATE TYPE [examples].[tt_id_varchar50] AS TABLE(
	[id] [varchar](50) NOT NULL,
	PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO
