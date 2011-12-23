package edu.umass.ciir.proteus

import akka.actor.Actor._
import akka.actor.Actor
import akka.dispatch.Future
import scala.collection.JavaConverters._
import edu.umass.ciir.proteus.protocol.ProteusProtocol._


/**
 * Class used by client programs to interact with the librarian/manager.
 */
class LibrarianClient(libHostName: String, libPort: Int) extends ProteusAPI {
	  
    // The connection to the librarian
	val librarian_actor = remote.actorFor(proteus_service_name, libHostName, libPort)
  
	/*** Query & Transform Methods ***/
	
	/**
	 * Queries the librarian for the query text (text) over the requested types (types_requested). 
	 * The result is a Future for a SearchResponse. The results of which can be then looked up to get 
	 * the full objects.
	 */
	def query(text: String, types_requested: List[ProteusType], 
			  num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	
		val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build
		
		val search_request = SearchRequest.newBuilder
				.setQuery(text)
				.setParams(search_params)
				.addAllTypes(types_requested.asJava)
				.build
				
		val search_message = Search.newBuilder
				.setSearchQuery(search_request)
				.build
				
		val response = librarian_actor ? search_message
		return response.mapTo[SearchResponse]
	} 

	/**
	 * A transformation query which gets the contents (reference requests) belonging to 
	 * a given access identifier (which specifies a data resource). The response is returned as a Future.
	 */
	def getContents(id: AccessIdentifier, id_type: ProteusType, contents_type: ProteusType, 
				num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	  
		if(!contents_map(id_type).contains(contents_type))
		  throw new IllegalArgumentException("Mismatched to/from types for getContents: (" + id_type.getValueDescriptor.getName + ", " + contents_type.getValueDescriptor.getName + ")")
		
		val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build  
				
		val transform_message = ContentsTransform.newBuilder
				.setId(id)
				.setFromType(id_type)
				.setToType(contents_type)
				.setParams(search_params)
				.build
				
		return (librarian_actor ? transform_message).mapTo[SearchResponse]
	}
	
	/**
	 * Get the reference result for the containing data resource of of this access identifier
	 */
	def getContainer(id: AccessIdentifier, id_type: ProteusType) : Future[SearchResponse] = {
	    val transform_message = ContainerTransform.newBuilder
			  	.setId(id)
			  	.setFromType(id_type)
			  	.build
			  
	    return (librarian_actor ? transform_message).mapTo[SearchResponse]
	}
	
	/**
	 * Get the overlaping resources of the same type as this one. Where the precise meaning of overlapping 
	 * is up to the end point data stores to decide.
	 */
	def getOverlaps(id: AccessIdentifier, id_type: ProteusType, num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	  	val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build  
				
		val transform_message = OverlapsTransform.newBuilder
			  	.setId(id)
			  	.setFromType(id_type)
			  	.setParams(search_params)
			  	.build
			  
	    return (librarian_actor ? transform_message).mapTo[SearchResponse] 
	}
	
	/**
	 * Get the references to Pages where this person, location, or organization identified by id, 
	 * occurs as an object of the provided term.
	 */
	def getOccurrencesAsObj(id: AccessIdentifier, id_type: ProteusType, term: String, 
							num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	  
	  	val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build  
				
		val transform_message = OccurAsObjTransform.newBuilder
			  	.setId(id)
			  	.setFromType(id_type)
			  	.setParams(search_params)
			  	.build
			  
	    return (librarian_actor ? transform_message).mapTo[SearchResponse] 
	}

	/**
	 * Get the references to Pages where this person, location, or organization identified by id, 
	 * occurs as a subject of the provided term.
	 */
	def getOccurencesAsSubj(id: AccessIdentifier, id_type: ProteusType, term: String, 
							num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	  
	  	val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build  
				
		val transform_message = OccurAsSubjTransform.newBuilder
			  	.setId(id)
			  	.setFromType(id_type)
			  	.setParams(search_params)
			  	.build
			  
	    return (librarian_actor ? transform_message).mapTo[SearchResponse] 
	}

	/**
	 * Get the references to Pages where this person, location, or organization identified by id, 
	 * occurs having as its object the provided term.
	 */
	def getOccurrencesHasObj(id: AccessIdentifier, id_type: ProteusType, term: String, 
							num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	  
	  	val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build  
				
		val transform_message = OccurHasObjTransform.newBuilder
			  	.setId(id)
			  	.setFromType(id_type)
			  	.setParams(search_params)
			  	.build
			  
	    return (librarian_actor ? transform_message).mapTo[SearchResponse] 
	}

	/**
	 * Get the references to Pages where this person, location, or organization identified by id, 
	 * occurs having as its subject the provided term.
	 */
	def getOccurrencesHasSubj(id: AccessIdentifier, id_type: ProteusType, term: String, 
							num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	  
	  	val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build  
				
		val transform_message = OccurHasSubjTransform.newBuilder
			  	.setId(id)
			  	.setFromType(id_type)
			  	.setParams(search_params)
			  	.build
			  
	    return (librarian_actor ? transform_message).mapTo[SearchResponse] 
	}

	/**
	 * Get locations within radius of the location described by id
	 */
	def getNearbyLocations(id: AccessIdentifier, radius: Int, 
							num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	  
	  	val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build  
				
		val transform_message = NearbyLocationsTransform.newBuilder
			  	.setId(id)
			  	.setRadiusMiles(radius)
			  	.setParams(search_params)
			  	.build
			  
	    return (librarian_actor ? transform_message).mapTo[SearchResponse] 
	}

	/**
	 * Use a dynamically loaded transform from id (of corresponding type id_type), where the name of the transform is 
	 * transform_name. The librarian must have a end point supporting this transform loaded for this to succeed.
	 */
	def useDynamicTransform(id: AccessIdentifier, id_type: ProteusType, transform_name: String,  
							num_requested: Int = 100, start_at: Int = 0, language: String = "en") : Future[SearchResponse] = {
	  
	  	val search_params = SearchParameters.newBuilder
				.setNumRequested(num_requested)
				.setStartAt(start_at)
				.setLanguage(language)
				.build  
				
		val dt_id = DynamicTransformID.newBuilder
				.setName(transform_name)
				.setFromType(id_type)
				.build
				
		val transform_message = DynamicTransform.newBuilder
			  	.setId(id)
			  	.setTransformId(dt_id)
			  	.setParams(search_params)
			  	.build
			  
	    return (librarian_actor ? transform_message).mapTo[SearchResponse] 
	}
	
	/*** Lookup Methods ***/
	
	/**
	 * Request that the librarian look up a Collection by its reference (SearchResult) and return 
	 * a Future to the full object.
	 */
	def lookupCollection(result: SearchResult) : Future[Collection] = {
		// Sanity checking first
		if (result.getProteusType != ProteusType.COLLECTION)
			throw new IllegalArgumentException("Mismatched type with lookup method")
	  
		val lookup_message = LookupCollection.newBuilder
	  			.setId(result.getId)
	  			.build
	  			
	  	return (librarian_actor ? lookup_message).mapTo[Collection]
	}
	
	/**
	 * Request the librarian look up a Page by its result reference
	 */
	def lookupPage(result: SearchResult) : Future[Page] = {
		// Sanity checking first
		if (result.getProteusType != ProteusType.PAGE)
			throw new IllegalArgumentException("Mismatched type with lookup method")
	  
		val lookup_message = LookupPage.newBuilder
	  			.setId(result.getId)
	  			.build
	  			
	  	return (librarian_actor ? lookup_message).mapTo[Page]
	}
	
	/**
	 * Request the librarian look up a Picture by its result reference
	 */
	def lookupPicture(result: SearchResult) : Future[Picture] = {
		// Sanity checking first
		if (result.getProteusType != ProteusType.PICTURE)
			throw new IllegalArgumentException("Mismatched type with lookup method")
	  
		val lookup_message = LookupPage.newBuilder
	  			.setId(result.getId)
	  			.build
	  			
	  	return (librarian_actor ? lookup_message).mapTo[Picture]
	}
	
	/**
	 * Request the librarian look up a Video by its result reference
	 */
	def lookupVideo(result: SearchResult) : Future[Video] = {
		// Sanity checking first
		if (result.getProteusType != ProteusType.VIDEO)
			throw new IllegalArgumentException("Mismatched type with lookup method")
	  
		val lookup_message = LookupPage.newBuilder
	  			.setId(result.getId)
	  			.build
	  			
	  	return (librarian_actor ? lookup_message).mapTo[Video]
	}
	
	/**
	 * Request the librarian look up a Audio clip by its result reference
	 */
	def lookupAudio(result: SearchResult) : Future[Audio] = {
		// Sanity checking first
		if (result.getProteusType != ProteusType.AUDIO)
			throw new IllegalArgumentException("Mismatched type with lookup method")
	  
		val lookup_message = LookupPage.newBuilder
	  			.setId(result.getId)
	  			.build
	  			
	  	return (librarian_actor ? lookup_message).mapTo[Audio]
	}
	
	/**
	 * Request the librarian look up a Person by its result reference
	 */
	def lookupPerson(result: SearchResult) : Future[Person] = {
		// Sanity checking first
		if (result.getProteusType != ProteusType.PERSON)
			throw new IllegalArgumentException("Mismatched type with lookup method")
	  
		val lookup_message = LookupPage.newBuilder
	  			.setId(result.getId)
	  			.build
	  			
	  	return (librarian_actor ? lookup_message).mapTo[Person]
	}
	
	/**
	 * Request the librarian look up a Location by its result reference
	 */
	def lookupLocation(result: SearchResult) : Future[Location] = {
		// Sanity checking first
		if (result.getProteusType != ProteusType.LOCATION)
			throw new IllegalArgumentException("Mismatched type with lookup method")
	  
		val lookup_message = LookupPage.newBuilder
	  			.setId(result.getId)
	  			.build
	  			
	  	return (librarian_actor ? lookup_message).mapTo[Location]
	}
	
	/**
	 * Request the librarian look up a Page by its result reference
	 */
	def lookupOrganization(result: SearchResult) : Future[Organization] = {
		// Sanity checking first
		if (result.getProteusType != ProteusType.ORGANIZATION)
			throw new IllegalArgumentException("Mismatched type with lookup method")
	  
		val lookup_message = LookupPage.newBuilder
	  			.setId(result.getId)
	  			.build
	  			
	  	return (librarian_actor ? lookup_message).mapTo[Organization]
	}

  /** Utility functions to make interacting with the data easier **/

  /**
   * Take a ResultSummary and turn it into a string with html tags around the
   * highlighted text regions.
   */
  def tagTerms(summary: ResultSummary, startTag: String = "<b>", endTag: String = "</b>") = wrapTerms(summary.getText, summary.getHighlightsList.asScala.toList, startTag=startTag, endTag=endTag)

  protected def wrapTerms(description: String, locations: List[TextRegion], startTag: String, endTag: String) : String = {
    if (locations.length == 0)
       return ""
    else if (locations.length == 1)
       return startTag + description.slice(locations(0).getStart, locations(0).getStop) + endTag +
    		   	description.slice(locations(0).getStop, description.length)
    else
       return startTag + description.slice(locations(0).getStart, locations(0).getStop) + endTag +
    		   	description.slice(locations(0).getStop, locations(1).getStart) + wrapTerms(description, locations.drop(1), startTag, endTag)
  }

}
