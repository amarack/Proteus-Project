package edu.umass.ciir.proteus
import akka.actor.Actor._
import akka.actor.Actor
import akka.remoteinterface._

import edu.umass.ciir.proteus.protocol.ProteusProtocol._

class BasicLibrarian extends 
	LibraryServer with 
	LibrarianConnectionManagement with 
	LibrarianQueryManagement with 
	LibrarianLookupManagement {
  
  	val serverHostname : String = "mildura.cs.umass.edu"
	val serverPort : Int = 8081

}

object basicLibrarianApp extends App {
  val librarianService = actorOf(new BasicLibrarian).start()
}

