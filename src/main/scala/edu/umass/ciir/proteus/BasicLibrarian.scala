package edu.umass.ciir.proteus
import akka.actor.Actor._
import akka.actor.Actor
import akka.remoteinterface._

import edu.umass.ciir.proteus.protocol.ProteusProtocol._

class BasicLibrarian(settings: ServerSetting) extends 
	LibraryServer with 
	LibrarianConnectionManagement with 
	LibrarianQueryManagement with 
	LibrarianLookupManagement {
  
  	val serverHostname : String = settings.hostname
	val serverPort : Int = settings.port

}

object basicLibrarianApp extends App {
  val librarianService = try {
    actorOf(new BasicLibrarian(ServerSetting(args(0), args(1).toInt))).start()
  } catch {
    case ex: Exception => 
	println("Unable to load hostname and port from arguments, defaulting to localhost:8081")
	actorOf(new BasicLibrarian(ServerSetting("localhost", 8081))).start()
  }
}

