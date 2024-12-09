USE [Enterprise32]
GO

IF OBJECT_ID('CopyPackageToDTrack', 'TR') IS NOT NULL
    DROP TRIGGER CopyPackageToDTrack;
GO

CREATE TRIGGER CopyPackageToDTrack
ON [dbo].[Package]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Allow explicit values to be inserted into the identity column
    SET IDENTITY_INSERT [DTrack].[dbo].[Package] ON;

    BEGIN TRY
        -- Insert new records or update existing ones in [DTrack].[dbo].[Package]
        MERGE [DTrack].[dbo].[Package] AS target
        USING (
            SELECT 
                [PackageID],
                [ShipmentNumber],
                [Quantity],
                [ShipMethod],
                [ShipDate],
                [ComponentNumber],
                [JobNumber],
                [NumberOfPackages],
                [Description],
                [TotalQtyShipped],
                [CreateDatim],
                [Updatedatim]
            FROM inserted
            WHERE [ShipMethod] = '10005'
        ) AS source
        ON target.[PackageID] = source.[PackageID] AND target.[ShipmentNumber] = source.[ShipmentNumber] AND target.[JobNumber] = source.[JobNumber]
        WHEN MATCHED THEN
            UPDATE SET
                target.[ShipmentNumber] = source.[ShipmentNumber],
                target.[Quantity] = source.[Quantity],
                target.[ShipMethod] = source.[ShipMethod],
                target.[ShipDate] = source.[ShipDate],
                target.[ComponentNumber] = source.[ComponentNumber],
                target.[JobNumber] = source.[JobNumber],
                target.[NumberOfPackages] = source.[NumberOfPackages],
                target.[Description] = source.[Description],
                target.[TotalQtyShipped] = source.[TotalQtyShipped],
                target.[CreateDatim] = source.[CreateDatim],
                target.[Updatedatim] = source.[Updatedatim]
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                [PackageID],
                [ShipmentNumber],
                [Quantity],
                [ShipMethod],
                [ShipDate],
                [ComponentNumber],
                [JobNumber],
                [NumberOfPackages],
                [Description],
                [TotalQtyShipped],
                [CreateDatim],
                [Updatedatim]
            )
            VALUES (
                source.[PackageID],
                source.[ShipmentNumber],
                source.[Quantity],
                source.[ShipMethod],
                source.[ShipDate],
                source.[ComponentNumber],
                source.[JobNumber],
                source.[NumberOfPackages],
                source.[Description],
                source.[TotalQtyShipped],
                source.[CreateDatim],
                source.[Updatedatim]
            );
    END TRY
    BEGIN CATCH
        -- Log the error
        INSERT INTO ErrorLog2 (
            ErrorMessage,
            ErrorSeverity,
            ErrorState,
            ErrorProcedure,
            ErrorLine
        )
        VALUES (
            ERROR_MESSAGE(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE()
        );

        -- Raise the error to the calling application
        THROW;
    END CATCH;

    -- Disable IDENTITY_INSERT
    SET IDENTITY_INSERT [DTrack].[dbo].[Package] OFF;
END;
GO


--- CopyPackageToDTrack.sql  will copy  [Enterprise32].[dbo].[Package] [DTrack].[dbo].[Package]