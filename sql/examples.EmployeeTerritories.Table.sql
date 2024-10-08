SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [examples].[EmployeeTerritories](
	[EmployeeID] [int] NOT NULL,
	[TerritoryID] [nvarchar](20) NOT NULL
) ON [PRIMARY]
GO
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (1, N'06897')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (1, N'19713')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (2, N'01581')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (2, N'01730')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (2, N'01833')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (2, N'02116')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (2, N'02139')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (2, N'02184')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (2, N'40222')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (3, N'30346')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (3, N'31406')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (3, N'32859')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (3, N'33607')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (4, N'20852')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (4, N'27403')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (4, N'27511')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (5, N'02903')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (5, N'07960')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (5, N'08837')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (5, N'10019')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (5, N'10038')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (5, N'11747')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (5, N'14450')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (6, N'85014')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (6, N'85251')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (6, N'98004')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (6, N'98052')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (6, N'98104')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'60179')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'60601')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'80202')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'80909')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'90405')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'94025')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'94105')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'95008')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'95054')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (7, N'95060')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (8, N'19428')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (8, N'44122')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (8, N'45839')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (8, N'53404')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (9, N'03049')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (9, N'03801')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (9, N'48075')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (9, N'48084')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (9, N'48304')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (9, N'55113')
INSERT [examples].[EmployeeTerritories] ([EmployeeID], [TerritoryID]) VALUES (9, N'55439')
GO
SET ANSI_PADDING ON
GO
ALTER TABLE [examples].[EmployeeTerritories] ADD  CONSTRAINT [PK_EmployeeTerritories] PRIMARY KEY NONCLUSTERED 
(
	[EmployeeID] ASC,
	[TerritoryID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [examples].[EmployeeTerritories]  WITH CHECK ADD  CONSTRAINT [FK_EmployeeTerritories_Employees] FOREIGN KEY([EmployeeID])
REFERENCES [examples].[Employees] ([EmployeeID])
GO
ALTER TABLE [examples].[EmployeeTerritories] CHECK CONSTRAINT [FK_EmployeeTerritories_Employees]
GO
ALTER TABLE [examples].[EmployeeTerritories]  WITH CHECK ADD  CONSTRAINT [FK_EmployeeTerritories_Territories] FOREIGN KEY([TerritoryID])
REFERENCES [examples].[Territories] ([TerritoryID])
GO
ALTER TABLE [examples].[EmployeeTerritories] CHECK CONSTRAINT [FK_EmployeeTerritories_Territories]
GO
