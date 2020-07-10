const {BigQuery} = require('@google-cloud/bigquery');
const moment = require('moment-timezone');
const geoTz = require('geo-tz')
const bigquery = new BigQuery();

async function mapToStream(stream, inFile, tz)
{
    let jp = JSON.parse(inFile);
    let array = jp.timelineObjects;

    array.map(x =>
        { 
            let y;
            if (x.placeVisit)
            {
                y = { type: "VISIT",
                    latStart: coordinate(x.placeVisit.location.latitudeE7),
                    lonStart: coordinate(x.placeVisit.location.longitudeE7),
                    latEnd: coordinate(x.placeVisit.location.latitudeE7),
                    lonEnd: coordinate(x.placeVisit.location.longitudeE7),
                    moveType: "NONE",
                    distance: 0,
                    semType: x.placeVisit.location.semanticType,
                    placeId: x.placeVisit.location.placeId,
                    address: x.placeVisit.location.address,
                    name: x.placeVisit.location.name,
                    confidence: x.placeVisit.location.confidence,
                    timestampStart: timestamp(x.placeVisit.duration.startTimestampMs),
                    timestampEnd: timestamp(x.placeVisit.duration.endTimestampMs),
                    latCenter: coordinate(x.placeVisit.centerLatE7),
                    lonCenter: coordinate(x.placeVisit.centerLngE7),
                    };

                if (y.centerLat && y.centerLon)
                {
                    y.pointCenter = geography(y.centerLat, y.centerLon);
                }
            }
            if (x.activitySegment)
            {

                y = { type: "TRAVEL",
                    latStart: coordinate(x.activitySegment.startLocation.latitudeE7),
                    lonStart: coordinate(x.activitySegment.startLocation.longitudeE7),
                    latEnd: coordinate(x.activitySegment.endLocation.latitudeE7),
                    lonEnd: coordinate(x.activitySegment.endLocation.longitudeE7),
                    moveType: x.activitySegment.activityType,
                    distance: x.activitySegment.distance,
                    timestampStart: timestamp(x.activitySegment.duration.startTimestampMs),
                    timestampEnd: timestamp(x.activitySegment.duration.endTimestampMs),
                    waypointPath: multipoint(x.activitySegment.waypointPath),
                    rawPath: linestring(x.activitySegment.simplifiedRawPath)

                    };
  
            }

            if (y.latStart && y.lonStart)
            {
                y.pointStart = geography(y.latStart, y.lonStart);
                y.date = timezone(y.timestampStart, y.latStart, y.lonStart);
            }
            else
            {
                y.date = timezone(y.timestampStart, null, null, tz);
            }

            if (y.latEnd && y.lonEnd)
            {
                y.pointEnd = geography(y.latEnd, y.lonEnd);
            }

            if (y.timestampEnd && y.timestampStart)
            {
                y.duration = (y.timestampEnd - y.timestampStart);
            }

            stream.write(`${JSON.stringify(y)}\n`)
        
    });

}


function coordinate(val)
{
    return val/10000000;
}

function timestamp(val)
{
    return parseInt(val/1000);
}

function timezone(val, lat, lon, tz)
{
    if (lat != null && lon != null)
    {
        [tz] = geoTz(lat, lon);
    }

    let unix = moment.unix(val);
    let date = moment.tz(unix, tz);

    let format = date.format('YYYY-MM-DD');
    return format;

}

function multipoint(p)
{
    let count = 0;
    let wp = "MULTIPOINT (";

    if (p)
    {
        p.waypoints.map(x => {
            if (count > 0)
            {
                wp = wp.concat(",");
            }
            let c = ` (${coordinate(x.lngE7)} ${coordinate(x.latE7)}) `;
            wp = wp.concat(c);

            count++;
        });
    }

    wp = wp.concat(")");
  
    if (count < 1) { wp = "MULTIPOINT EMPTY"; }

    return wp;
}

function linestring(p)
{
    let count = 0;
    let rp = "LINESTRING (";
    
    if (p)
    {
        p.points.map(x => {

            let c = `${coordinate(x.lngE7)} ${coordinate(x.latE7)}`;

            if (rp.indexOf(c) < 0)
            {
                if (count > 0)
                {
                    rp = rp.concat(",")
                }
                
                rp = rp.concat(c);
                count++;
            }
        });

    }

    rp = rp.concat(")");

    if (count <= 1) { rp = "LINESTRING EMPTY"; }

    return rp;
}

function geography(lat, lon)
{
    return `POINT(${lon} ${lat})`;
}

module.exports = { mapToStream };