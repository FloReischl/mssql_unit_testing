SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [examples].[Territories](
	[TerritoryID] [nvarchar](20) NOT NULL,
	[TerritoryDescription] [nchar](50) NOT NULL,
	[RegionID] [int] NOT NULL
) ON [PRIMARY]
GO
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'01581', N'Westboro                                          ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'01730', N'Bedford                                           ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'01833', N'Georgetow                                         ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'02116', N'Boston                                            ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'02139', N'Cambridge                                         ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'02184', N'Braintree                                         ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'02903', N'Providence                                        ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'03049', N'Hollis                                            ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'03801', N'Portsmouth                                        ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'06897', N'Wilton                                            ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'07960', N'Morristown                                        ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'08837', N'Edison                                            ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'10019', N'New York                                          ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'10038', N'New York                                          ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'11747', N'Mellvile                                          ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'14450', N'Fairport                                          ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'19428', N'Philadelphia                                      ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'19713', N'Neward                                            ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'20852', N'Rockville                                         ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'27403', N'Greensboro                                        ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'27511', N'Cary                                              ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'29202', N'Columbia                                          ', 4)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'30346', N'Atlanta                                           ', 4)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'31406', N'Savannah                                          ', 4)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'32859', N'Orlando                                           ', 4)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'33607', N'Tampa                                             ', 4)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'40222', N'Louisville                                        ', 1)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'44122', N'Beachwood                                         ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'45839', N'Findlay                                           ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'48075', N'Southfield                                        ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'48084', N'Troy                                              ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'48304', N'Bloomfield Hills                                  ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'53404', N'Racine                                            ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'55113', N'Roseville                                         ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'55439', N'Minneapolis                                       ', 3)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'60179', N'Hoffman Estates                                   ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'60601', N'Chicago                                           ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'72716', N'Bentonville                                       ', 4)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'75234', N'Dallas                                            ', 4)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'78759', N'Austin                                            ', 4)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'80202', N'Denver                                            ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'80909', N'Colorado Springs                                  ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'85014', N'Phoenix                                           ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'85251', N'Scottsdale                                        ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'90405', N'Santa Monica                                      ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'94025', N'Menlo Park                                        ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'94105', N'San Francisco                                     ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'95008', N'Campbell                                          ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'95054', N'Santa Clara                                       ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'95060', N'Santa Cruz                                        ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'98004', N'Bellevue                                          ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'98052', N'Redmond                                           ', 2)
INSERT [examples].[Territories] ([TerritoryID], [TerritoryDescription], [RegionID]) VALUES (N'98104', N'Seattle                                           ', 2)
GO
SET ANSI_PADDING ON
GO
ALTER TABLE [examples].[Territories] ADD  CONSTRAINT [PK_Territories] PRIMARY KEY NONCLUSTERED 
(
	[TerritoryID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [examples].[Territories]  WITH CHECK ADD  CONSTRAINT [FK_Territories_Region] FOREIGN KEY([RegionID])
REFERENCES [examples].[Region] ([RegionID])
GO
ALTER TABLE [examples].[Territories] CHECK CONSTRAINT [FK_Territories_Region]
GO
