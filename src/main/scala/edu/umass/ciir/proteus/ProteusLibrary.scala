package edu.umass.ciir.proteus
import akka.dispatch._
import akka.actor.Actor._
import akka.actor.Actor
import akka.config.Supervision._
import scala.collection.JavaConverters._

import edu.umass.ciir.proteus.protocol.ProteusProtocol._

// ServerSetting is used to pass around the hostname and port for connecting to some server.
case class ServerSetting(hostname: String, port: Int)

/**
 * The base trait for scala object model of the Proteus API.
 * Provides implicit conversion between strings and proteus types, as well as 
 * information (maps) about the type hierarchy. Also provides the name of the service that 
 * proteus actors are supporting.
 */
trait ProteusAPI {
    implicit def str2ProteusType(str: String) = convertType(str)
  	
  	val proteus_service_name = "library-service" 
	def numProteusTypes : Int = 8
	
	// What a given type can have contents of type
	val contents_map = Map(ProteusType.COLLECTION 	-> List(ProteusType.PAGE),	// Collection contains Pages 
						   ProteusType.PAGE 		-> List(ProteusType.PICTURE,ProteusType.VIDEO,ProteusType.AUDIO,ProteusType.PERSON,ProteusType.LOCATION,ProteusType.ORGANIZATION),
						   ProteusType.PICTURE 		-> List(ProteusType.PERSON,ProteusType.LOCATION,ProteusType.ORGANIZATION),		// Picture contains entities
						   ProteusType.VIDEO 		-> List(ProteusType.PERSON,ProteusType.LOCATION,ProteusType.ORGANIZATION),		// Video contains entities
						   ProteusType.AUDIO 		-> List(ProteusType.PERSON,ProteusType.LOCATION,ProteusType.ORGANIZATION))
			
	val container_map = Map(ProteusType.PAGE -> List(ProteusType.COLLECTION), 
							ProteusType.PICTURE -> List(ProteusType.PAGE), 
							ProteusType.VIDEO -> List(ProteusType.PAGE), 
							ProteusType.AUDIO -> List(ProteusType.PAGE), 
							ProteusType.PERSON -> List(ProteusType.PAGE, ProteusType.PICTURE, ProteusType.VIDEO, ProteusType.AUDIO), 
							ProteusType.LOCATION -> List(ProteusType.PAGE, ProteusType.PICTURE, ProteusType.VIDEO, ProteusType.AUDIO), 
							ProteusType.ORGANIZATION -> List(ProteusType.PAGE, ProteusType.PICTURE, ProteusType.VIDEO, ProteusType.AUDIO))
							
    def containerFor(ptype: ProteusType) : List[ProteusType] = if(container_map.contains(ptype)) container_map(ptype) else null
							
	def convertType(strType: String) : ProteusType = strType match {
		  case "collection" => ProteusType.COLLECTION
		  case "page" => ProteusType.PAGE
		  case "picture" => ProteusType.PICTURE
		  case "video" => ProteusType.VIDEO
		  case "audio" => ProteusType.AUDIO
		  case "person" => ProteusType.PERSON
		  case "location" => ProteusType.LOCATION
		  case "organization" => ProteusType.ORGANIZATION
		  case _ => throw new IllegalArgumentException("Invalid proteus type: " + strType)
	}
	
	def convertTypes(types: List[String]) : List[ProteusType] = {
		types.map(convertType(_))
	}
					
}

/**
 * The base trait for the librarian and for the end point (library). 
 */
trait LibraryServer extends Actor with ProteusAPI {

	def serverHostname : String
	def serverPort : Int
	
	override def preStart() = {
	    remote.start(serverHostname, serverPort)
              .register(proteus_service_name, self)
	}
  
    // Actor message handler
	def receive: Receive = connectionManagement orElse queryManagement orElse lookupManagement
	
	// Abstract methods to be defined elsewhere
	protected def connectionManagement : Receive
	protected def queryManagement : Receive
	protected def lookupManagement : Receive
	
	protected def supportsType(ptype: ProteusType) : Boolean
	protected def supportsDynTransform(dtID: DynamicTransformID) : Boolean

}

/**
 * Trait for generating pseudo random strings and keys.
 */
trait RandomDataGenerator {
    val keyChars: String = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).mkString("")
    /**
     * Generates a random alpha-numeric string of fixed length (8).
     * Granted, this is a BAD way to do it, because it doesn't guarantee true randomness.
     */
    def genKey(length: Int = 8): String = (1 to length).map(x => keyChars.charAt(util.Random.nextInt(keyChars.length))).mkString
  
}

/**
 * Abstract base class which contains abstract methods for all the methods that a data store (library/endpoint) trait 
 * needs to implement.
 */
abstract trait EndPointDataStore {
  	/** Methods Used Here and Elsewhere (MUST BE PROVIDED) **/
  	protected def getSupportedTypes : List[ProteusType]
  	protected def getDynamicTransforms : List[DynamicTransformID]
	
	/** Core Functionality Methods (MUST BE PROVIDED) **/
    protected def supportsType(ptype: ProteusType) : Boolean
	protected def supportsDynTransform(dtID: DynamicTransformID) : Boolean
	
  	protected def runSearch(s: Search) : SearchResponse
  	
  	protected def runContainerTransform(transform: ContainerTransform) : SearchResponse
  	protected def runContentsTransform(transform: ContentsTransform) : SearchResponse
  	protected def runOverlapsTransform(transform: OverlapsTransform) : SearchResponse
  	protected def runOccurAsObjTransform(transform: OccurAsObjTransform) : SearchResponse
  	protected def runOccurAsSubjTransform(transform: OccurAsSubjTransform) : SearchResponse
  	protected def runOccurHasObjTransform(transform: OccurHasObjTransform) : SearchResponse
  	protected def runOccurHasSubjTransform(transform: OccurHasSubjTransform) : SearchResponse
  	protected def runNearbyLocationsTransform(transform: NearbyLocationsTransform) : SearchResponse
  	protected def runDynamicTransform(transform: DynamicTransform) : SearchResponse
  	
  	protected def lookupCollection(accessID: AccessIdentifier) : Collection
    protected def lookupPage(accessID: AccessIdentifier) : Page
    protected def lookupPicture(accessID: AccessIdentifier) : Picture
    protected def lookupVideo(accessID: AccessIdentifier) : Video
    protected def lookupAudio(accessID: AccessIdentifier) : Audio
    protected def lookupPerson(accessID: AccessIdentifier) : Person
    protected def lookupLocation(accessID: AccessIdentifier) : Location
    protected def lookupOrganization(accessID: AccessIdentifier) : Organization
}
