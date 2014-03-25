package org.corespring.river.mongodb;

/**
 * Container class for constants related to standards objects.
 */
public class Standard {

  // MongoDB collection containing standards
  public static final String COLLECTION = "ccstandards";

  // Keys for values in the standards records
  public class Keys {
    public static final String CATEGORY = "category";
    public static final String DOT_NOTATION = "dotNotation";
    public static final String STANDARD = "standard";
    public static final String SUBCATEGORY = "subCategory";
  }

}
