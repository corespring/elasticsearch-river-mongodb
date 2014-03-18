package org.corespring.river.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryOperators;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.mongodb.MongoDBRiver;

import java.util.ArrayList;
import java.util.List;


/**
 * A utility class to convert {@link Object}s to/from  Corespring versioned id {@link String}s.
 */
public class VersionedIdHelper {

  private static final ESLogger logger = ESLoggerFactory.getLogger(VersionedIdHelper.class.getName());

  /** Field in a {@link DBObject} representing the versioned id **/
  static final String MONGODB_VERSION_FIELD = "version";
  static final String MONGODB_VERSIONED_VERSION_FIELD =
    MongoDBRiver.MONGODB_ID_FIELD + "." + MongoDBRiver.MONGODB_ID_FIELD;

  /**
   * Takes a {@link Object}, either representing an {@link ObjectId} or a {@link DBObject}, and converts it to a
   * {@link String} representing the appropriate Corespring versioned {@link ObjectId}.
   */
  public static String versionedIdString(Object object) {
    if (object == null) {
      return null;
    }
    else if (object instanceof DBObject) {
      return getVersionedId((DBObject) object);
    } else if (object instanceof ObjectId) {
      return object.toString();
    } else {
      throw new IllegalArgumentException("Cannot parse _id object");
    }
  }

  /**
   * Returns the id piece of a {@link String} representing a versioned {@link ObjectId}. For example, the versioned id
   * {@link String} "530e078118a78ca06c0c1ff7:4" would have the result "530e078118a78ca06c0c1ff7".
   * @param id
   * @return
   */
  public static ObjectId getId(Object id) {
    if (id instanceof String) {
      String idString = (String) id;
      if (idString.split(":").length > 1) {
        return new ObjectId(idString.split(":")[0]);
      } else {
        return new ObjectId(idString);
      }
    } else if (id instanceof ObjectId) {
      return (ObjectId) id;
    } else if (id instanceof BSONObject) {
      BSONObject bsonObject = (BSONObject) id;
      if (bsonObject.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
        if (bsonObject.get(MongoDBRiver.MONGODB_ID_FIELD) instanceof ObjectId) {
          return (ObjectId) bsonObject.get(MongoDBRiver.MONGODB_ID_FIELD);
        } else {
          throw new IllegalArgumentException(
            "Object of type " + id.getClass() + "did not have " + MongoDBRiver.MONGODB_ID_FIELD + "of type " + ObjectId.class.toString());
        }
      } else {
        throw new IllegalArgumentException(
          "Object of type " + id.getClass() + " missing field " + MongoDBRiver.MONGODB_ID_FIELD);
      }
    } else {
      throw new IllegalArgumentException("Object of type " + id.getClass() + " cannot be converted to ObjectId");
    }
  }

  /**
   * Creates a greater-than query for looking for at versioned ids.
   */
  public static BasicDBObject gtQuery(String id) {
    List<BasicDBObject> conditions = new ArrayList<BasicDBObject>(2);

    conditions.add(new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, new BasicBSONObject(QueryOperators.GT, id)));
    conditions.add(new BasicDBObject(MONGODB_VERSIONED_VERSION_FIELD, new BasicBSONObject(QueryOperators.GT, id)));

    return new BasicDBObject(QueryOperators.OR, conditions);
  }

  /**
   * Rewrites the id field of a {@link BSONObject} to be non-versioned.
   */
  public static DBObject unversionId(DBObject dbObject) {
    if (dbObject.containsField(MongoDBRiver.MONGODB_ID_FIELD)) {
      try {
        dbObject.put(MONGODB_VERSION_FIELD, getVersion(dbObject));
        dbObject.put(MongoDBRiver.MONGODB_ID_FIELD, getId(dbObject.get(MongoDBRiver.MONGODB_ID_FIELD)));
      } catch (Exception e) {
        logger.error("Error", e);
      }
    }
    return dbObject;
  }

  private static Object getVersion(DBObject dbObject) throws Exception {
    if (dbObject.containsField(MONGODB_VERSION_FIELD)) {
      return getFromBSON(dbObject, MONGODB_VERSION_FIELD);
    } else if (dbObject.containsField(MongoDBRiver.MONGODB_ID_FIELD) &&
      dbObject.get(MongoDBRiver.MONGODB_ID_FIELD) instanceof DBObject) {
      return getVersion(getFromBSON(dbObject, MongoDBRiver.MONGODB_ID_FIELD, DBObject.class));
    } else {
      return 0;
    }
  }

  /**
   * Given a {@link DBObject} object of the form:
   *
   * <pre>
   *   {
   *     "_id": ObjectId("530dfcce18a78ca06c0c1ff6"),
   *     "version": 0
   *   }
   * </pre>
   *
   * This method will convert it to the String representation "530dfcce18a78ca06c0c1ff6". If the object does not have
   * the appropriate "_id" field, an {@link IllegalArgumentException} will be thrown.
   */
  private static String getVersionedId(DBObject dbObject) {
    String objectId = getFromBSON(dbObject, MongoDBRiver.MONGODB_ID_FIELD, ObjectId.class).toString();
    return objectId.toString();
  }

  private static Object getFromBSON(DBObject dbObject, String field) {
    return getFromBSON(dbObject, field, Object.class);
  }

  /**
   * Takes a {@link DBObject}, looks for a particular field, attempts to cast it to the provided {@link Class} and
   * return. If any of that fails, it throws {@link IllegalArgumentException}s.
   */
  private static <T> T getFromBSON(DBObject dbObject, String field, Class<T> clazz) {
    if (dbObject.containsField(field)) {
      try {
        return clazz.cast(dbObject.get(field));
      } catch (ClassCastException e) {
        throw new IllegalArgumentException("Field " + field + " did not match type " + clazz.toString() +
          ", matched " + dbObject.get(field).getClass());
      }
    } else {
      throw new IllegalArgumentException("DBObject is missing field " + field);
    }
  }

}
