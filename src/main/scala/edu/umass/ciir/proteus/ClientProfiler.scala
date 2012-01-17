package edu.umass.ciir.proteus

import akka.actor.Actor._
import akka.actor.Actor
import akka.dispatch.Future
import scala.collection.JavaConverters._
import edu.umass.ciir.proteus.protocol.ProteusProtocol._

object clientProfilerApp extends App {
  val client = new LibrarianClient(args(0), args(1).toInt)
  
  // Run some queries...
  for (i <- 0 until 1000) {
    // Do a query
    val results = client.query("Some random text " + i.toString, 
        client.convertTypes(List("collection", "page", "person", "location", "picture"))).get
    
    // Look up some of the results
    val parallel_requests = results.getResultsList.asScala.map(result => {
      val lookup = result.getProteusType match {
        case ProteusType.COLLECTION => client.lookupCollection(result)
        case ProteusType.PAGE => client.lookupPage(result)
        case ProteusType.PERSON => client.lookupPerson(result)
        case ProteusType.PICTURE => client.lookupPicture(result)
        case ProteusType.LOCATION => client.lookupLocation(result)
        case _ => Future {None}
      }
      val contents = result.getProteusType match {
        case ProteusType.COLLECTION => client.getContents(result.getId, result.getProteusType, ProteusType.PAGE)
        case ProteusType.PAGE => client.getContainer(result.getId, result.getProteusType, ProteusType.COLLECTION)
        case ProteusType.PERSON => client.getContainer(result.getId, result.getProteusType, ProteusType.PAGE)
        case ProteusType.PICTURE => client.getContainer(result.getId, result.getProteusType, ProteusType.PAGE)
        case ProteusType.LOCATION => client.getContainer(result.getId, result.getProteusType, ProteusType.PAGE)
        case _ => Future {None}
      }
      (lookup, contents)})
      
    parallel_requests.map(r => (r._1.get, r._2.get))
  }
}