var express = require('express');
const request = require('request');
const cassandra  = require('cassandra-driver');
const fileUpload = require('express-fileupload');
var csv = require('csv');
const formidable = require('formidable');
var querystring = require('querystring');
var rbush = require('rbush');
var knn = require('rbush-knn');
var app = express();
app.set('view engine', 'ejs');
app.use(fileUpload());
var tree = rbush(30, ['[0]', '[1]', '[0]', '[1]']); // accept [x, y] points
var router = express.Router();
const client = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    localDataCenter: 'datacenter1'
});
client.connect(function (err, result) {
    console.log('Connection to Cassandra successful');
});

router.get('/', function (req, res) {
    res.render('home');
});

//single point insert code starts
router.post('/pointInsert', function(req, res) {
    var fid = req.body.fid;
    console.log(fid);
    var id = req.body.id;
    var name = req.body.name;
    var city = req.body.city;
    var state = req.body.state;
    var zip = req.body.zip;
    var county = req.body.county;
    var fips = req.body.fips;
    var naicsdescr = req.body.naicsdescr;
    var xCoord = req.body.x;
    var yCoord = req.body.y;
    var concatenatedXY = yCoord.toString()+xCoord.toString();
    var hrstart = process.hrtime()
    tree.insert([yCoord, xCoord]);
    var cqlInsertSampleData =
        "INSERT INTO cs298.railroadbridges (" +
        "   xcoord, ycoord, " +
        "   city, county," +
        "   fid, fips, id," +
        "   naicsdescr, name, state," +
        "   zip, concatenatedxy" +
        ") VALUES (?,?, ?,?, ?,?,?, ?,?,?, ?,?)";
    client.execute(
        cqlInsertSampleData,
        [   xCoord, yCoord, city, county,
            fid, fips, id,
            naicsdescr, name, state,
            zip, concatenatedXY
        ], { prepare : true },
        {hints: [
                null, null, null, null,
                null, null, null,
                null, null, null,
                null, null
            ]},
        function (err, result) {
            if (err) {
                console.log('Insert Sample Data Failed: ' + JSON.stringify(err));
                // needed to add this to get the error message as sometimes stringify didn't include it
                console.log('Insert Sample Data Failed: ' + err.message);
            } else {
                console.log('Inserted Sample Data: ' + JSON.stringify(result));
            }
        }
    );
    hrend = process.hrtime(hrstart);
    console.log("Execution time for single insertion %ds %dms", hrend[0], hrend[1] / 1000000)
    res.redirect("/");
});
//single point insert code ends

//bulk load code starts
router.post('/bulk', function(req, res) {
    new formidable.IncomingForm().parse(req, (err, fields, files) => {
        if (err) {
            console.error('Error', err);
            throw err
        }
        var hrstart;
        var objectCSV = csv();
        hrstart = process.hrtime()
        objectCSV.from.path(files.csvFile.path, {
            delimiter: ",",
            escape: '"'
        })
            .on("record", function(row, index) {
                var fid, id, name, city, state, zip, county, fips, naicsdescr, xCoord, yCoord, concatenatedXY;

                // skip the header row
                if (index === 0) {
                    return;
                }

                fid = row[0].trim();
                id = row[1].trim();
                name = row[2].trim();
                city = row[3].trim();
                state = row[4].trim();
                zip = row[5].trim();
                county = row[6].trim();
                fips = row[7].trim();
                naicsdescr = row[8].trim();
                xCoord = row[9].trim();
                yCoord = row[10].trim();
                concatenatedXY =  yCoord.toString()+xCoord.toString();
                tree.insert([yCoord, xCoord]);
                console.log(tree.toJSON())
                var cqlInsertSampleData =
                    "INSERT INTO cs298.railroadbridges (" +
                    "   xcoord, ycoord, " +
                    "   city, county," +
                    "   fid, fips, id," +
                    "   naicsdescr, name, state," +
                    "   zip, concatenatedxy" +
                    ") VALUES (?,?, ?,?, ?,?,?, ?,?,?, ?,?)";
                client.execute(
                    cqlInsertSampleData,
                    [   xCoord, yCoord, city, county,
                        fid, fips, id,
                        naicsdescr, name, state,
                        zip, concatenatedXY
                    ], { prepare : true },
                    {hints: [
                            null, null, null, null,
                            null, null, null,
                            null, null, null,
                            null, null
                        ]},
                    function (err, result) {
                        if (err) {
                            console.log('Insert Sample Data Failed: ' + JSON.stringify(err));
                            // needed to add this to get the error message as sometimes stringify didn't include it
                            console.log('Insert Sample Data Failed: ' + err.message);
                        } else {
                            console.log('Inserted Sample Data: ' + JSON.stringify(result));
                        }
                    }
                );

            })
            .on("end", function() {
                // redirect back to the root
                hrend = process.hrtime(hrstart);
                console.log("Execution time for bulk load is %ds %dms", hrend[0], hrend[1] / 1000000)
                res.redirect("/");
            })
            .on("error", function(error) {
                console.log(error.message);
            })
    });
});
//bulk load code ends

//Delete one code starts
router.post('/deleteSingle', function(req, res) {
    console.log("Here")
    var x = req.body.x;
    var y = req.body.y;
    var value = y.toString()+x.toString();
    console.log(value)
    const query = 'DELETE FROM cs298.railroadbridges WHERE concatenatedxy = ?';
    var hrstart = process.hrtime()
    tree.remove(x,y);
    client.execute(query, [value], { prepare : true })
        .then(result => {
            console.log('Data is deleted')
            hrend = process.hrtime(hrstart);
            console.log("Execution time for delete one is %ds %dms", hrend[0], hrend[1] / 1000000)
            res.redirect("/");
            });
});
//Delete one code ends

//Delete all code starts
router.post('/deleteAll', function(req, res) {
    const query = 'TRUNCATE TABLE cs298.railroadbridges;';
    hrstart = process.hrtime()
    tree.clear();
    client.execute(query, { prepare : true })
        .then(result => {
            console.log('Truncated the table')
            hrend = process.hrtime(hrstart);
            console.log("Execution time for delete all is %ds %dms", hrend[0], hrend[1] / 1000000)
            res.redirect("/");
        });

});
//Delete all code ends

//search code starts
router.post('/search', function(req, res) {
    var minx = req.body.minx;
    var miny = req.body.miny;
    var maxx = req.body.maxx;
    var maxy = req.body.maxy;
    var concatenatedXY=[];
    var hrstart = process.hrtime()
    var result = tree.search({minX: miny, minY: minx, maxX: maxy, maxY: maxx});
    console.log(result)
    console.log(concatenatedXY)
    for(var x =0;x<result.length;x++){
            concatenatedXY[x] = result[x][1].toString()+result[x][0].toString() ;
    }
    console.log(concatenatedXY)
    if(concatenatedXY.length) {
        const query = 'SELECT * FROM cs298.railroadbridges WHERE concatenatedxy IN ?';
        client.execute(query, [concatenatedXY], {prepare: true})
            .then(result => {
                hrend = process.hrtime(hrstart);
                console.log("Execution time for overlaps is %ds %dms", hrend[0], hrend[1] / 1000000)
                res.redirect("/");
                console.log('User with email %s', JSON.stringify(result))});
    }
});
//search code ends

//Intersects code starts
router.post('/intersects', function(req, res) {
    var minx = req.body.minx;
    var miny = req.body.miny;
    var maxx = req.body.maxx;
    var maxy = req.body.maxx;
    var hrstart = process.hrtime()
    var result = tree.collides({minX: miny, minY: minx, maxX: maxy, maxY: maxx});
    console.log("Is query BBox intersecting any point(s)?",result)
    hrend = process.hrtime(hrstart);
    console.log("Execution time for overlaps is %ds %dms", hrend[0], hrend[1] / 1000000)
    res.redirect("/");
});
//Intersects code ends

//knn code starts
router.post('/knn', function(req, res) {
    var x = req.body.x;
    var y = req.body.y;
    var k = req.body.k;
    var concatenatedXY=[];
    const query = 'SELECT * FROM cs298.railroadbridges WHERE concatenatedxy IN ?';
    var hrstart = process.hrtime()
    var neighbors = knn(tree, y, x, k);
    for(var x =0;x<result.length;x++){
        concatenatedXY[x] =  neighbors[x][1].toString()+neighbors[x][0].toString();
    }
    client.execute(query, [concatenatedXY], { prepare : true })
        .then(result => {
            hrend = process.hrtime(hrstart);
            res.redirect("/");
            console.log('K nearest neighbor query result: %s', JSON.stringify(result))
            console.log("Execution time for knn is %ds %dms", hrend[0], hrend[1] / 1000000)
        });
});
//knn code ends
module.exports = router;


    