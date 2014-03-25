package org.corespring.river.mongodb;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;

import java.util.*;

/**
 * A simple data access object used for retrieving standards from the database.
 */
public class StandardsDAO {

  private final Collection<Map> standards;

  public StandardsDAO(Mongo mongoClient, MongoDBRiverDefinition definition) {
    DBCollection standardsCollection = mongoClient.getDB(definition.getMongoDb()).getCollection(Standard.COLLECTION);
    this.standards = getStandards(standardsCollection);
  }

  private Collection<Map> getStandards(DBCollection standardsCollection) {
    Collection<Map> standards = new ArrayList<Map>();
    for (Iterator<DBObject> iterator = standardsCollection.find().iterator(); iterator.hasNext();) {
      DBObject object = iterator.next();
      standards.add(object.toMap());
    }
    return standards;
  }

  /**
   * Returns all standards contained within the database
   */
  public Collection<Map> getStandards() {
    return this.standards;
  }


}
