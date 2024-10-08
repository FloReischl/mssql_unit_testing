SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [examples].[Region](
	[RegionID] [int] NOT NULL,
	[RegionDescription] [nchar](50) NOT NULL
) ON [PRIMARY]
GO
INSERT [examples].[Region] ([RegionID], [RegionDescription]) VALUES (1, N'Eastern                                           ')
INSERT [examples].[Region] ([RegionID], [RegionDescription]) VALUES (2, N'Western                                           ')
INSERT [examples].[Region] ([RegionID], [RegionDescription]) VALUES (3, N'Northern                                          ')
INSERT [examples].[Region] ([RegionID], [RegionDescription]) VALUES (4, N'Southern                                          ')
GO
ALTER TABLE [examples].[Region] ADD  CONSTRAINT [PK_Region] PRIMARY KEY NONCLUSTERED 
(
	[RegionID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
