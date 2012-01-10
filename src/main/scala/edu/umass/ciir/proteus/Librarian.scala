package edu.umass.ciir.proteus
import akka.actor.Actor._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.config.Supervision._
import akka.dispatch._
import scala.collection.JavaConverters._

import edu.umass.ciir.proteus.protocol.ProteusProtocol._

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
 * Connection management for the librarian. 
 */
trait LibrarianConnectionManagement extends RandomDataGenerator { this: Actor =>
  
    // This case class provides a data structure for details of a single end point connection (single library)
    case class ActiveLibrary(connection: ConnectLibrary, groupId: String, library_actor: ActorRef)
    
    // Hashmaps for quickly looking up the library/endpoint from its key
  	val libraries = new collection.mutable.HashMap[String, ActiveLibrary]()
  	// Libraries with the same groupID can support each other and combine to form a single resource
  	val group_membership = new collection.mutable.HashMap[String, List[String]]()
  	
  	var supported_types = Set[ProteusType]()
  	var supported_dyn_transforms = List[DynamicTransformID]()

  	// Get the actor reference for a library with the id given
  	protected def getLibraryActor(id: String) : ActorRef = if (libraries.contains(id)) libraries(id).library_actor else null
  	
  	/**
  	 * Determines if the library should be referred to by a new generated key, 
  	 * the one it requested, or if there is a collision and the connection is trashed.
  	 */
    protected def libKeyID(c: ConnectLibrary) : String = {
      if (!c.hasRequestedKey)
        genKey()
      else {
        val reqKey = c.getRequestedKey
        if (libraries.contains(reqKey)) {
          val collision_conn = libraries(reqKey).connection
          if (collision_conn.getHostname == c.getHostname && 
              collision_conn.getPort == c.getPort && 
              collision_conn.getGroupId == c.getGroupId)
            return reqKey
          else
        	return ""
        } else 
          return reqKey
      }
    }
     
    /**
     * Handle messages for connecting to end points for the librarian.
     */
	protected def connectionManagement : Receive = {
    	case c: ConnectLibrary =>
    		// Generate a new identifier for this library
    	    val id = libKeyID(c)
    	    val lib_actor = remote.actorFor("library-service", c.getHostname, c.getPort)
    	    
    	    if (id == "") { // Conflict on keys, refuse connection
    	    	val connected_message = LibraryConnected.newBuilder
    	    		.setError("Requested Key conflicts with existing incompatible server")
    	    		.build
    	    		
    	    	lib_actor ! connected_message
    	    } else {
	    	    // Add group membership
	    	    if (!c.hasGroupId) {
	    	        val gID = genKey()
	    	    	group_membership += gID -> List(id)
	    	    	libraries += id -> ActiveLibrary(c, gID, lib_actor)
	    	    }
	    	    else if (group_membership.contains(c.getGroupId)) {
	    	        group_membership(c.getGroupId) ::= id
	    	    	libraries += id -> ActiveLibrary(c, c.getGroupId, lib_actor)
	    	    }
	    	    else {
	    	        group_membership += c.getGroupId -> List(id)
	    	    	libraries += id -> ActiveLibrary(c, c.getGroupId, lib_actor)
	    	    }
	    	    
	    	    // Update support types/transforms
	    	    supported_types ++= Set() ++ c.getSupportedTypesList.asScala
	    	    supported_dyn_transforms ++= c.getDynamicTransformsList.asScala
	    	    
	    	    // Tell the library we have them connected
	    	    val connected_message = LibraryConnected.newBuilder
	    	    		.setResourceId(id)
	    	    		.build
	    	    		
	    	    lib_actor ! connected_message
    	    }
    	    
  	}
    
	// Get the group id for a given library (by pulling out its id from an access identifier)
    protected def getGroupId(accessID: AccessIdentifier) : String = libraries(accessID.getResourceId).groupId
    
    protected def groupMemberTypeSupport(ptype: ProteusType, groupId: String) : List[String] = {
      group_membership(groupId).filter(id => libraries(id).connection.getSupportedTypesList.asScala.contains(ptype))
    }
    
    protected def groupMemberDynTransSupport(dtID: DynamicTransformID, groupId: String) : List[String] = {
      group_membership(groupId).filter(id => 
        libraries(id).connection.getDynamicTransformsList.asScala.exists(dt => 
          dt.getName == dtID.getName && dt.getFromType == dtID.getFromType))
    }
    
    /**
     * The main workhorse of this trait, which conditionally forwards messages to libraries (whose ids are in list members), 
     * or sends the message to all of them and recombines the results, alternatively may also return an error based response.
     * This is where most of the interesting Librarian intelligent code would go (such as load balancing and topic grouping).
     */
    protected def sendOrForwardTo(members: List[String], message: Any) : Future[SearchResponse] = {
        if (members.length == 0) {
    	  	return Future { SearchResponse.newBuilder
    	  			  		.setError("No library support for this operation...")
    	  			  		.build 
    	  			  		}
        } else if (members.length == 1) {
          // In current version of Akka this is a broken feature (try again later)
//          libraries(members(0)).library_actor forward message
//          return null
          // Instead we can do:
          return (libraries(members(0)).library_actor ? message).mapTo[SearchResponse]
        } else {
          // All the re-ordering code for reassembling the multiple responses should go here
          val futureList = members.map(libraries(_).library_actor).map(a => (a ? message).mapTo[SearchResponse])
          val resultsList = Futures.fold(List[SearchResult]())(futureList)((a: List[SearchResult], b: SearchResponse) => b.getResultsList.asScala.toList ::: a)
          val final_result = resultsList
          		.map(srList => SearchResponse.newBuilder.addAllResults(srList.asJava).build)
          		.recover {
	    	  	    case _ => SearchResponse.newBuilder
	    	  			  		.setError("Error in responses from libraries...")
	    	  			  		.build
    	  	  	}
          return final_result
        }
    }

    protected def typeSupport(ptypes: List[ProteusType]) : List[String] = {
      libraries.keys.toList.filter(k => libraries(k).connection.getSupportedTypesList.asScala.exists(t => ptypes.contains(t)))
    }
    
    protected def supportsType(ptype: ProteusType) : Boolean = supported_types.contains(ptype)
	protected def supportsDynTransform(dtID: DynamicTransformID) : Boolean = supported_dyn_transforms.contains(dtID)
  
}

/**
 * Trait providing query management and transform management for the librarian class
 */
trait LibrarianQueryManagement { this: Actor =>
  
    /**
     * All these message handlers do essentially the same thing (which is a work around for the bug), 
     * and that is to call sendOrForwardTo, and then send that response back to the client.
     */
  	protected def queryManagement : Receive = {
    	case s: Search =>
    	  	val search_query = s.getSearchQuery
    	  	val members = typeSupport(search_query.getTypesList.asScala.toList)
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, s)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
    	  	
    
    	// Libraries must be able to transform to its contained type when getting contents, and from its type when getting container
      case trans: ContainerTransform => // If you support the from type, then you must be able to get its container
        val members = groupMemberTypeSupport(trans.getFromType, getGroupId(trans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, trans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
        
      case trans: ContentsTransform => // If you support the To Type then you must be able to get the contents (equivalent statement as above)
        val members = groupMemberTypeSupport(trans.getToType, getGroupId(trans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, trans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
        
      case trans: OverlapsTransform =>
        val members = groupMemberTypeSupport(trans.getFromType, getGroupId(trans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, trans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
        
      case trans: OccurAsObjTransform => 
        val members = groupMemberTypeSupport(trans.getFromType, getGroupId(trans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, trans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
        
      case trans: OccurAsSubjTransform => 
        val members = groupMemberTypeSupport(trans.getFromType, getGroupId(trans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, trans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
        
      case trans: OccurHasObjTransform => 
        val members = groupMemberTypeSupport(trans.getFromType, getGroupId(trans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, trans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
        
      case trans: OccurHasSubjTransform =>
        val members = groupMemberTypeSupport(trans.getFromType, getGroupId(trans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, trans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
        
      case trans: NearbyLocationsTransform =>
        val members = groupMemberTypeSupport(ProteusType.LOCATION, getGroupId(trans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, trans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
        
      case dtrans: DynamicTransform =>
        val members = groupMemberDynTransSupport(dtrans.getTransformId, getGroupId(dtrans.getId))
    	  	val chan = self.channel
    	  	val future_result = sendOrForwardTo(members, dtrans)
    	  	if (future_result != null)
    	  		future_result onResult { case r: SearchResponse => chan ! r }
  	}
    	  	
  	protected def groupMemberDynTransSupport(dtID: DynamicTransformID, groupId: String) : List[String]
  	protected def getGroupId(accessID: AccessIdentifier) : String
  	protected def typeSupport(ptypes: List[ProteusType]) : List[String]
  	protected def groupMemberTypeSupport(ptype: ProteusType, groupId: String) : List[String]
  	protected def sendOrForwardTo(members: List[String], message: Any) : Future[SearchResponse]
}

/**
 * Provide lookup management for the librarian.
 */
trait LibrarianLookupManagement { this: Actor =>
  
    /**
     * Same bug issue as for query management trait, same solution.
     */
  	protected def lookupManagement : Receive = {
    	case lookup: LookupCollection =>
    	  	val lib_actor = getLibraryActor(lookup.getId.getResourceId)
    	  	if(lib_actor == null)
    	  		self.reply(Collection.newBuilder
    	  		    .setId(AccessIdentifier.newBuilder
    					  .setIdentifier(lookup.getId.getIdentifier)
    					  .setResourceId(lookup.getId.getResourceId)
    					  .setError("Received lookup with unrecognized resource ID: " + lookup.getId.getResourceId)
    					  .build)
    			  .build)
    	    else { // This is simply a placeholder until the remote actor Akka bug gets fixed (supposedly was just fixed in latest version, def. by 2.0)
    	      val chan = self.channel
    	      (lib_actor ? lookup) onResult { case r => chan ! r }
    	    }
//    	  	else lib_actor forward lookup
    	  	
    	case lookup: LookupPage =>
    	  	val lib_actor = getLibraryActor(lookup.getId.getResourceId)
    	  	if(lib_actor == null)
    	  		self.reply(Page.newBuilder
    	  		    .setId(AccessIdentifier.newBuilder
    					  .setIdentifier(lookup.getId.getIdentifier)
    					  .setResourceId(lookup.getId.getResourceId)
    					  .setError("Received lookup with unrecognized resource ID: " + lookup.getId.getResourceId)
    					  .build)
    			  .build)
    	  	else {
    	      val chan = self.channel
    	      (lib_actor ? lookup) onResult { case r => chan ! r }
    	    }
//    	  	else lib_actor forward lookup
    	  	
    	case lookup: LookupPicture =>
    	  	val lib_actor = getLibraryActor(lookup.getId.getResourceId)
    	  	if(lib_actor == null)
    	  		self.reply(Picture.newBuilder
    	  		    .setId(AccessIdentifier.newBuilder
    					  .setIdentifier(lookup.getId.getIdentifier)
    					  .setResourceId(lookup.getId.getResourceId)
    					  .setError("Received lookup with unrecognized resource ID: " + lookup.getId.getResourceId)
    					  .build)
    			  .build)
    	  	else {
    	      val chan = self.channel
    	      (lib_actor ? lookup) onResult { case r => chan ! r }
    	    }
//    	  	else lib_actor forward lookup
    	  	
    	case lookup: LookupVideo =>
    	  	val lib_actor = getLibraryActor(lookup.getId.getResourceId)
    	  	if(lib_actor == null)
    	  		self.reply(Video.newBuilder
    	  		    .setId(AccessIdentifier.newBuilder
    					  .setIdentifier(lookup.getId.getIdentifier)
    					  .setResourceId(lookup.getId.getResourceId)
    					  .setError("Received lookup with unrecognized resource ID: " + lookup.getId.getResourceId)
    					  .build)
    			  .build)
    	  	else {
    	      val chan = self.channel
    	      (lib_actor ? lookup) onResult { case r => chan ! r }
    	    }
//    	  	else lib_actor forward lookup
    	  	
    	case lookup: LookupAudio =>
    	  	val lib_actor = getLibraryActor(lookup.getId.getResourceId)
    	  	if(lib_actor == null)
    	  		self.reply(Audio.newBuilder
    	  		    .setId(AccessIdentifier.newBuilder
    					  .setIdentifier(lookup.getId.getIdentifier)
    					  .setResourceId(lookup.getId.getResourceId)
    					  .setError("Received lookup with unrecognized resource ID: " + lookup.getId.getResourceId)
    					  .build)
    			  .build)
    	  	else {
    	      val chan = self.channel
    	      (lib_actor ? lookup) onResult { case r => chan ! r }
    	    }
//    	  	else lib_actor forward lookup
    	  	
    	case lookup: LookupPerson =>
    	  	val lib_actor = getLibraryActor(lookup.getId.getResourceId)
    	  	if(lib_actor == null)
    	  		self.reply(Person.newBuilder
    	  		    .setId(AccessIdentifier.newBuilder
    					  .setIdentifier(lookup.getId.getIdentifier)
    					  .setResourceId(lookup.getId.getResourceId)
    					  .setError("Received lookup with unrecognized resource ID: " + lookup.getId.getResourceId)
    					  .build)
    			  .build)
    	  	else {
    	      val chan = self.channel
    	      (lib_actor ? lookup) onResult { case r => chan ! r }
    	    }
//    	  	else lib_actor forward lookup
    	  	
    	case lookup: LookupLocation =>
    	  	val lib_actor = getLibraryActor(lookup.getId.getResourceId)
    	  	if(lib_actor == null)
    	  		self.reply(Location.newBuilder
    	  		    .setId(AccessIdentifier.newBuilder
    					  .setIdentifier(lookup.getId.getIdentifier)
    					  .setResourceId(lookup.getId.getResourceId)
    					  .setError("Received lookup with unrecognized resource ID: " + lookup.getId.getResourceId)
    					  .build)
    			  .build)
    	  	else {
    	      val chan = self.channel
    	      (lib_actor ? lookup) onResult { case r => println("replying with location " + r); chan ! r }
    	    }
//    	  	else lib_actor forward lookup
    	  	
    	case lookup: LookupOrganization =>
    	  	val lib_actor = getLibraryActor(lookup.getId.getResourceId)
    	  	if(lib_actor == null)
    	  		self.reply(Organization.newBuilder
    	  		    .setId(AccessIdentifier.newBuilder
    					  .setIdentifier(lookup.getId.getIdentifier)
    					  .setResourceId(lookup.getId.getResourceId)
    					  .setError("Received lookup with unrecognized resource ID: " + lookup.getId.getResourceId)
    					  .build)
    			  .build)
    	  	else {
    	      val chan = self.channel
    	      (lib_actor ? lookup) onResult { case r => chan ! r }
    	    }
//    	  	else lib_actor forward lookup

  	}
  	
  	protected def getLibraryActor(id: String) : ActorRef
}


