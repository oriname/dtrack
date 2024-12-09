USE [DTrack];
GO

-- Drop the trigger if it already exists
IF OBJECT_ID('UpdateShipmentOnDelivery', 'TR') IS NOT NULL
    DROP TRIGGER UpdateShipmentOnDelivery;
GO

-- Create a new trigger that fires after updates on dtrack_shipping
CREATE TRIGGER UpdateShipmentOnDelivery
ON [DTrack].[dbo].[dtrack_shipping]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Check if the job has been marked as completed and Shipped is still NULL or 0
        IF EXISTS (
            SELECT 1
            FROM inserted
            WHERE Detrack_Status = 'completed' 
              AND (Shipped IS NULL OR Shipped = 0)
        )
        BEGIN
            -- 1. Update Shipped in the DTrack.Shipping table
            UPDATE [DTrack].[dbo].[dtrack_shipping]
            SET Shipped = 1
            WHERE Detrack_Status = 'completed'
              AND (Shipped IS NULL OR Shipped = 0)
              AND EXISTS (SELECT 1 FROM inserted WHERE Detrack_Status = 'completed');

            -- 2. Update Shipped in the DTrack.Shipment table for matching JobNumber
            UPDATE S
            SET S.Shipped = 1
            FROM [DTrack].[dbo].[Shipment] AS S
            INNER JOIN inserted AS i
                ON S.JobNumber = i.do_number
            WHERE i.Detrack_Status = 'completed' 
              AND (i.Shipped IS NULL OR i.Shipped = 0);

            -- 3. Update Shipped in the Enterprise32.Shipment table for matching JobNumber
            UPDATE E
            SET E.Shipped = 1
            FROM [Enterprise32].[dbo].[Shipment] AS E
            INNER JOIN inserted AS i
                ON E.JobNumber = i.do_number
            WHERE i.Detrack_Status = 'completed' 
              AND (i.Shipped IS NULL OR i.Shipped = 0);
        END

    END TRY
    BEGIN CATCH
        -- Log the error in ErrorLog2
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

        -- Re-throw the error to the calling application
        THROW;
    END CATCH;

END;
GO
