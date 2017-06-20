#!/usr/bin/env node

var util = require("util");
var configfile = require('config-file');
var winston = require('winston');
var CachemanMemory = require('cacheman-memory');
const cluster = require('cluster');
var rest = require('restler');



var config = configfile(__dirname + '/../config.json') || {};

config.loglevel = config.loglevel || 'debug';
config.logfile = config.logfile || '/var/log/node-sbc.log';


logger = new winston.Logger({
    transports: [
        new winston.transports.File({
            level: config.loglevel,
            filename: config.logfile,
            handleExceptions: true,
            json: false,
            maxsize: 104857600,
            maxFiles: 8,
            colorize: false,
            humanReadableUnhandledException: true
        }),
        new winston.transports.Console({
            level: "error",
            handleExceptions: true,
            json: false,
            colorize: true,
            humanReadableUnhandledException: true
        })
    ],
    exitOnError: false
});

var cache = new CachemanMemory();



if( !config || !isObject(config) || !Object.keys(config).length() ){
	logger.error("Failed to start. Config file was missing or could not be parsed");
	process.exit();
}

if( !config.database or !config.database.length() ){
	logger.error("Failed to start. Config file was missing or could not be parsed");
	process.exit();
}


var exec_state = "RUNNING";





if (cluster.isMaster) {

	process.title = "Node-SBC";

	logger.info("Starting Node-SBC...");

	workers = {};

	//create_worker( args );

	setInterval( function(){ logger.debug("Master process is alive.."); },30000);

	// Graceful Shutdown
	process.on("SIGTERM", function(){ logger.warn("SIGTERM received.. shutting down.."); do_shutdown(); } );
	process.on("SIGINT", function(){ logger.warn("SIGINT received.. shutting down.."); do_shutdown(); } );
}




if( cluster.isWorker ){

	metrics = {};
	env = process.env;
	run_worker( env );
}





function create_worker( args ){

	logger.debug("Spawning new worker");

	worker_env = {};

	var worker = cluster.fork( worker_env );

	if( worker && worker.id ){
		logger.info("New worker added: " + worker.id );
		var created_at = unixtime();

		workers[ worker.id ] = {
			"id": worker.id,
			"created_at": created_at
		};
	}

	// handle messages sent to Master from the worker
	worker.on('message', function(payload){ processMessageFromWorker( worker.id, payload ); });

	return worker;
}





function run_worker( env ){

	logger.debug("Thread running");

	process.title = "Worker";

	logger = new winston.Logger({
		transports: [
			new winston.transports.File({
				level: 'debug',
				filename: '/var/log/node-sbc-workers-debug.log',
				handleExceptions: true,
				json: false,
				maxsize: 104857600,
				maxFiles: 8,
				colorize: true,
				humanReadableUnhandledException: true
			})
		],
		exitOnError: false
	});

	logger.info("Starting worker");

	process.on('message', function(payload){ processMessageFromMaster(payload); });

	process.on("SIGTERM", function(){ logger.warn("Worker received SIGTERM.. shutting down"); do_shutdown(); } );
	process.on("SIGINT", function(){ logger.warn("Worker received SIGINT.. shutting down"); do_shutdown(); } );

	// do something..

	setInterval( function(){ report_metrics(); },15000);
}














































function processMessageFromWorker( worker_id, payload ){

	logger.debug("Message received from Worker " + worker_id );
	logger.debug("Message content: " + JSON.stringify(payload) );

	if( payload && isObject(payload) && payload.cmd ){

		if( payload.cmd == "notify_shutdown" ){

			logger.warn("Worker #" + worker_id + " for " + workers[ worker_id ].server + " is shutting down..");

		}else if( payload.cmd == "metrics" ){

			if( payload.metrics ){

				workers[ worker_id ].metrics = payload.metrics;

				logger.debug("Worker #" + worker_id + " is reporting metrics");
			}
		}
	}
}



function processMessageFromMaster( payload ){

	if( payload && isObject(payload) && payload.cmd ){

		if( payload.cmd == "ping" ){

			process.send({
				cmd: "pong",
				unixtime: unixtime(),
			});

		}else if( payload.cmd == "shutdown" || payload.cmd == "SIGINT" || payload.cmd == "SIGTERM" ) {

			logger.info("Master has sent a message to shutdown..");
			do_shutdown();
		}
	}
}



function unixtime(){
    t = Math.floor(new Date() / 1000);
    return t;
}



function isObject( x ) {
	if (x === null) {
		return false;
	}
	return ( (typeof x === 'function') || (typeof x === 'object') );
}



function in_array(needle, haystack) {
    for(var i in haystack) {
        if(haystack[i] == needle) return true;
    }
    return false;
}



function is_json(){
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}



function report_metrics(){

	if( cluster.isWorker ){

		metrics.last_sent = unixtime();

		process.send({
			cmd: "metrics",
			metrics: metrics,
		});
	}
}



function do_shutdown(){

	logger.warn('running shutdown()');

	exec_state = "EXITING";

	if( cluster.isMaster ){

		for( wid in cluster.workers ){

			logger.warn('Stopping Worker ID ' + wid + '..');

			cluster.workers[wid].send({ "cmd": "shutdown" } );

			// if the works don't reply, then kill them..
			setTimeout( function(){ if( cluster.workers[wid] ){ cluster.workers[wid].kill('SIGKILL'); } }, 8000);
		}

		logger.warn('Shutting down..');
		process.exit(1);

	}

	if( cluster.isWorker ){
		process.send( { cmd: "notify_shutdown" } );
		process.exit(1);
	}
}



