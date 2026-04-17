SELECT
    CONCAT(location, '-', CAST(System.Timestamp AS nvarchar(max))) AS id,
    location,
    System.Timestamp AS windowEnd,
    AVG(iceThicknessCm) AS avgIceThicknessCm,
    MIN(iceThicknessCm) AS minIceThicknessCm,
    MAX(iceThicknessCm) AS maxIceThicknessCm,
    AVG(surfaceTemperatureC) AS avgSurfaceTemperatureC,
    MIN(surfaceTemperatureC) AS minSurfaceTemperatureC,
    MAX(surfaceTemperatureC) AS maxSurfaceTemperatureC,
    MAX(snowAccumulationCm) AS maxSnowAccumulationCm,
    AVG(externalTemperatureC) AS avgExternalTemperatureC,
    COUNT(*) AS readingCount,
    CASE
        WHEN AVG(iceThicknessCm) >= 30 AND AVG(surfaceTemperatureC) <= -2 THEN 'Safe'
        WHEN AVG(iceThicknessCm) >= 25 AND AVG(surfaceTemperatureC) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetyStatus
INTO SensorAggregations
FROM iotinput
GROUP BY
    location,
    TumblingWindow(minute, 5);

SELECT
    location,
    System.Timestamp AS windowEnd,
    AVG(iceThicknessCm) AS avgIceThicknessCm,
    MIN(iceThicknessCm) AS minIceThicknessCm,
    MAX(iceThicknessCm) AS maxIceThicknessCm,
    AVG(surfaceTemperatureC) AS avgSurfaceTemperatureC,
    MIN(surfaceTemperatureC) AS minSurfaceTemperatureC,
    MAX(surfaceTemperatureC) AS maxSurfaceTemperatureC,
    MAX(snowAccumulationCm) AS maxSnowAccumulationCm,
    AVG(externalTemperatureC) AS avgExternalTemperatureC,
    COUNT(*) AS readingCount,
    CASE
        WHEN AVG(iceThicknessCm) >= 30 AND AVG(surfaceTemperatureC) <= -2 THEN 'Safe'
        WHEN AVG(iceThicknessCm) >= 25 AND AVG(surfaceTemperatureC) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetyStatus
INTO bloboutput
FROM iotinput
GROUP BY
    location,
    TumblingWindow(minute, 5);SELECT
    CONCAT(location, '-', CAST(System.Timestamp AS nvarchar(max))) AS id,
    location,
    System.Timestamp AS windowEnd,
    AVG(iceThicknessCm) AS avgIceThicknessCm,
    MIN(iceThicknessCm) AS minIceThicknessCm,
    MAX(iceThicknessCm) AS maxIceThicknessCm,
    AVG(surfaceTemperatureC) AS avgSurfaceTemperatureC,
    MIN(surfaceTemperatureC) AS minSurfaceTemperatureC,
    MAX(surfaceTemperatureC) AS maxSurfaceTemperatureC,
    MAX(snowAccumulationCm) AS maxSnowAccumulationCm,
    AVG(externalTemperatureC) AS avgExternalTemperatureC,
    COUNT(*) AS readingCount,
    CASE
        WHEN AVG(iceThicknessCm) >= 30 AND AVG(surfaceTemperatureC) <= -2 THEN 'Safe'
        WHEN AVG(iceThicknessCm) >= 25 AND AVG(surfaceTemperatureC) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetyStatus
INTO SensorAggregations
FROM iotinput
GROUP BY
    location,
    TumblingWindow(minute, 5);

SELECT
    location,
    System.Timestamp AS windowEnd,
    AVG(iceThicknessCm) AS avgIceThicknessCm,
    MIN(iceThicknessCm) AS minIceThicknessCm,
    MAX(iceThicknessCm) AS maxIceThicknessCm,
    AVG(surfaceTemperatureC) AS avgSurfaceTemperatureC,
    MIN(surfaceTemperatureC) AS minSurfaceTemperatureC,
    MAX(surfaceTemperatureC) AS maxSurfaceTemperatureC,
    MAX(snowAccumulationCm) AS maxSnowAccumulationCm,
    AVG(externalTemperatureC) AS avgExternalTemperatureC,
    COUNT(*) AS readingCount,
    CASE
        WHEN AVG(iceThicknessCm) >= 30 AND AVG(surfaceTemperatureC) <= -2 THEN 'Safe'
        WHEN AVG(iceThicknessCm) >= 25 AND AVG(surfaceTemperatureC) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetyStatus
INTO bloboutput
FROM iotinput
GROUP BY
    location,
    TumblingWindow(minute, 5);