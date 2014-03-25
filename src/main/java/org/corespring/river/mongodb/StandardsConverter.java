package org.corespring.river.mongodb;

import com.mongodb.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * This class is designed to take an existing content object with a standard attribute representing an array of
 * dot notations for standards, and replace that array with objects containing the matching standard's metadata. See
 * the {@link #addStandardData(com.mongodb.DBObject)} method for more information.
 *
 * Why do this over using a separate index for the standards and referencing that? It turns out that Elasticsearch is
 * (at the time of this writing) not especially good at handling parent/child, many-to-many relationships. The
 * researched solutions involve making queries to both indexes and then piecing the data back together manually. The
 * following seemed like a more elegant solution, being that the standards data is relatively small in size, and
 * more-or-less static.
 */
public class StandardsConverter {

  public class ContentKeys {
    public static final String STANDARDS = "standards";
  }

  // Fields to include in the resultant data conversion.
  private static final String[] STANDARD_FIELDS =
    { Standard.Keys.CATEGORY, Standard.Keys.DOT_NOTATION, Standard.Keys.STANDARD, Standard.Keys.SUBCATEGORY };

  private final Collection<Map> standards;

  public StandardsConverter(StandardsDAO standardsDAO) {
    this.standards = standardsDAO.getStandards();
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
          Map standard = getStandardByDotNotation(dotNotation);
          if (standard != null) {
            newStandards.add(filterStandard(standard));
          }
        }
      }

      if (!newStandards.isEmpty()) {
        source.put(ContentKeys.STANDARDS, newStandards);
      }
    }
    return source;
  }

  /**
   * Returns a standard with the provided dot notation, null if not present.
   */
  private Map getStandardByDotNotation(String dotNotation) {
    for (Map standard : standards) {
      if (standard.containsKey(Standard.Keys.DOT_NOTATION) &&
        standard.get(Standard.Keys.DOT_NOTATION).equals(dotNotation)) {
        return standard;
      }
    }
    return null;
  }

  /**
   * Returns a version of the provided {@link DBObject} with only STANDARD_FIELDS' fields.
   */
  private DBObject filterStandard(Map standard) {
    DBObject object = new BasicDBObject();
    for (String field : STANDARD_FIELDS) {
      addIfExists(field, standard, object);
    }
    return object;
  }


  /**
   * Adds the field from source to target if present.
   */
  private void addIfExists(String field, Map source, DBObject target) {
    if (source.containsKey(field)) {
      target.put(field, source.get(field));
    }
  }

}
