package org.corespring.river.mongodb;

import com.mongodb.*;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class is designed to take an existing content object with a standard attribute representing an array of
 * dot notations for standards, and replace that array with objects containing the matching standard's metadata. See
 * the {@link #addStandardData(com.mongodb.DBObject)} method for more information.
 */
public class StandardsConverter {

  public class StandardKeys {
    public static final String CATEGORY = "category";
    public static final String DOT_NOTATION = "dotNotation";
    public static final String STANDARD = "standard";
    public static final String SUBCATEGORY = "subCategory";
  }

  public class ContentKeys {
    public static final String STANDARDS = "standards";
  }

  private static final String STANDARDS_COLLECTION = "ccstandards";

  // Fields to include in the resultant data conversion.
  private static final String[] STANDARD_FIELDS =
    { StandardKeys.CATEGORY, StandardKeys.DOT_NOTATION, StandardKeys.STANDARD, StandardKeys.SUBCATEGORY };

  // Mapping of standards by their dot-notation
  private final Map<String, DBObject> standards;

  public StandardsConverter(Mongo mongoClient, MongoDBRiverDefinition definition) {
    DBCollection standardsCollection = mongoClient.getDB(definition.getMongoDb()).getCollection(STANDARDS_COLLECTION);
    this.standards = getStandards(standardsCollection);
  }

  private Map<String, DBObject> getStandards(DBCollection standardsCollection) {
    Map<String, DBObject> standards = new HashMap<String, DBObject>();
    for (Iterator<DBObject> iterator = standardsCollection.find().iterator(); iterator.hasNext();) {
      DBObject object = iterator.next();
      standards.put(object.get(StandardKeys.DOT_NOTATION).toString(), filterStandard(object));
    }
    return standards;
  }

  /**
   * Returns a version of the provided {@link DBObject} with only STANDARD_FIELDS' fields.
   */
  private DBObject filterStandard(DBObject standard) {
    DBObject object = new BasicDBObject();
    for (String field : STANDARD_FIELDS) {
      addIfExists(field, standard, object);
    }
    return object;
  }

  /**
   * Adds the field from source to target if present.
   */
  private void addIfExists(String field, DBObject source, DBObject target) {
    if (source.containsField(field)) {
      target.put(field, source.get(field));
    }
  }

  /**
   * Replaces a standards list of dot notations with expanded values of standards. For example:
   *
   * <pre>
   *   {
   *     ...
   *     "standards" : [ "4.OA.B.4", "4.OA.C.5" ],
   *     ...
   *   }
   * </pre>
   *
   * becomes
   *
   * <pre>
   *   {
   *     ...
   *     "standards" : [
   *       {
   *         "category" : "Operations & Algebraic Thinking",
   *         "dotNotation" : "4.OA.B.4",
   *         "standard" : "Find all factor pairs for a whole number in the range 1&ndash;100...",
   *         "subCategory" : "Gain familiarity with factors and multiples."
   *       },
   *       {
   *         "category" : "Operations & Algebraic Thinking",
   *         "dotNotation" : "4.OA.C.5",
   *         "standard" : "Generate a number or shape pattern that follows a given rule...",
   *         "subCategory" : "Generate and analyze patterns."
   *       }
   *     ]
   *     ...
   *   }
   * </pre>
   */
  public DBObject addStandardData(DBObject source) {
    if (source.containsField(ContentKeys.STANDARDS) && source.get(ContentKeys.STANDARDS) instanceof BasicDBList) {
      BasicDBList newStandards = new BasicDBList();
      BasicDBList standardsList = (BasicDBList) source.get(ContentKeys.STANDARDS);

      for (Iterator<Object> iterator = standardsList.iterator(); iterator.hasNext();) {
        Object obj = iterator.next();
        if (obj instanceof String) {
          String dotNotation = (String) obj;
          if (this.standards.containsKey(dotNotation)) {
            newStandards.add(this.standards.get(dotNotation));
          }
        }
      }

      if (!newStandards.isEmpty()) {
        source.put(ContentKeys.STANDARDS, newStandards);
      }
    }
    return source;
  }


}
