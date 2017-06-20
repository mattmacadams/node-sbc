#!/usr/bin/env node

var util = require("util");
var configfile = require('config-file');
var winston = require('winston');
var CachemanMemory = require('cacheman-memory');
const cluster = require('cluster');
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var rest = require('restler');
var OBJ = require('obj-tools');



var config = configfile(__dirname + '/../config.json') || {};

config.logging.level = config.logging.level || 'debug';
config.logging.file = config.logging.file || '/var/log/node-sbc.log';
config.console.level = config.console.level || 'debug';


logger = new winston.Logger({
	transports: [
		new winston.transports.File({
			level: config.logging.level,
			filename: config.logging.file,
			handleExceptions: true,
			json: false,
			maxsize: 104857600,
			maxFiles: 8,
			colorize: false,
			humanReadableUnhandledException: true
		}),
		new winston.transports.Console({
			level: config.console.level || "debug",
			handleExceptions: true,
			json: false,
			colorize: true,
			humanReadableUnhandledException: true
		})
	],
	exitOnError: false
});

module.exports = logger;

module.exports.stream = {
	write: function(message, encoding){
		logger.info(message);
	}
};


var cache = new CachemanMemory();



if( !config || !isObject(config) || !Object.keys(config).length ){
	logger.error("Failed to start. Config file was missing or could not be parsed");
	process.exit();
}

if( !config.database || !isObject(config.database) ){
	logger.error("Failed to start. The 'database' config parameter missing or could not be parsed");
	process.exit();
}

if( !config.database.url ){
	logger.error("Failed to start. The 'database.url' config parameter missing or could not be parsed");
	process.exit();
}



var exec_state = "RUNNING";



if (cluster.isMaster) {
	workers = {};
	init();
}



if( cluster.isWorker ){
	metrics = {};
	go_worker( process.env );
}












function init(){

	logger.info("Starting Node-SBC...");

	process.title = "Node-SBC";

	// Graceful Shutdown
	process.on("SIGTERM", function(){ logger.warn("SIGTERM received.. shutting down.."); do_shutdown(); } );
	process.on("SIGINT", function(){ logger.warn("SIGINT received.. shutting down.."); do_shutdown(); } );

	// SBC Settings
	settings = {};

	// MongoDB Collections
	collections = {};

	// connect to database
	MongoClient.connect( config.database.url, {
		"poolSize": (config.database.poolSize || 8)
	}, function(err, db) {

		if( err ){
			logger.error("Failed to connect to MongoDB: " + err);
			do_shutdown();
		}

		// load collections
		collections.settings = db.collection('settings');
		collections.metrics = db.collection('metrics');
		collections.logs = db.collection('logs');

		collections.settings.find().addQueryModifier('$orderby', {'_id':-1}).limit(1).next( function(err, settings) {

			if( err ){
				logger.error("Failed to load settings from MongoDB: " + err);
				do_shutdown();
			}

			if( isObject(settings) ){
				logger.info("Using settings version " + settings['_id'] + " updated at " + settings['timestamp'] );
			}
		});

		setInterval( function(){ monitor_settings(); },60000);
		setInterval( function(){ logger.debug("Master process is alive.."); },30000);
	});
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





function go_worker( env ){

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
		if(haystack[i] == needle){
			return true;
		}
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



function update_settings( new_settings ){

	if( OBJ.isObject(new_settings) ){

		collections.settings.find().addQueryModifier('$orderby', {'_id':-1}).limit(1).next( function(err, existing_settings) {

			if( !err ){
				existing_settings = existing_settings || {};

				var new_settings = OBJ.merge({}, existing_settings, new_settings, { "_id": undefined, "version": ((existing_settings.version+1) || 1), "timestamp": unixtime() } );
				//x._id = undefined;
				logger.info("Updating settings in MongoDB..");
				collections.settings.insertOne(new_settings);
			}
		});
	}
}



function monitor_settings(){

	logger.debug("Checking for new version of config");

	// todo: if settings version is greater than current

	if( validate_settings( ) ){
		broadcast_config();
	}
}



function validate_settings( obj ){
	// ensure cfg looks like a valid

	// todo

	return TRUE;
}



function broadcast_config(){
	// send updated config to all Workers

	// todo
	return TRUE;
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
