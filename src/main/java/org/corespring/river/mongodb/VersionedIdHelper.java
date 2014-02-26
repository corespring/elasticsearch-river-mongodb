package org.corespring.river.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryOperators;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.elasticsearch.river.mongodb.MongoDBRiver;

import java.util.ArrayList;
import java.util.List;


/**
 * A utility class to convert {@link Object}s to/from  Corespring versioned id {@link String}s.
 */
public class VersionedIdHelper {

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
    } else {
      throw new IllegalArgumentException("Object of type " + id.getClass() + " cannot be converted to ObjectId");
    }
  }

  public static BasicDBObject gtQuery(String id) {
    List<BasicDBObject> conditions = new ArrayList<BasicDBObject>(2);

    conditions.add(new BasicDBObject(MongoDBRiver.MONGODB_ID_FIELD, new BasicBSONObject(QueryOperators.GT, id)));
    conditions.add(new BasicDBObject(MONGODB_VERSIONED_VERSION_FIELD, new BasicBSONObject(QueryOperators.GT, id)));

    return new BasicDBObject(QueryOperators.OR, conditions);
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
   * This method will convert it to the String representation "530dfcce18a78ca06c0c1ff6:0". If the object does not have
   * the appropriate "_id" and "version" fields, an {@link IllegalArgumentException} will be thrown.
   */
  private static String getVersionedId(DBObject dbObject) {
    String objectId = getFromBSON(dbObject, MongoDBRiver.MONGODB_ID_FIELD, String.class);
    String version = getFromBSON(dbObject, MONGODB_VERSION_FIELD);
    return objectId.toString() + ":" + version.toString();
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
        throw new IllegalArgumentException("Field " + field + " did not match type " + clazz.toString());
      }
    } else {
      throw new IllegalArgumentException("DBObject is missing field " + field);
    }
  }

  /**
   * Get the {@link String} representation of the provided field in the {@link DBObject}. If not present, throws
   * {@link IllegalArgumentException}.
   */
  private static String getFromBSON(DBObject dbObject, String field) {
    if (dbObject.containsField(field)) {
      return dbObject.get(field).toString();
    } else {
      throw new IllegalArgumentException("DBObject is missing field " + field);
    }
  }

}
