package com.ms.silverking.text;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.io.StreamParser;
import com.ms.silverking.log.Log;


/**
 * Parses object definitions.
 * 
 * E.g. field1=val1,field2=val2, etc.
 * 
 * JSON-like, but specialized and limited to usage in silverking. 
 * (Could migrate implementation to JSON.)
 */
public class ObjectDefParser<T> {
    private final T                 template;
    private final Class<T>          _class;
    private final Constructor<T>    constructor;
    private final Field[]           fields;
    private final FieldsRequirement  fieldRequirement;
    private final NonFatalExceptionResponse nonFatalExceptionResponse;
    private final String    fieldDefDelimiter;
    private final String    nameValueDelimiter;
    private final Set<String>   optionalFields;
    private final Set<String>   exclusionFields;
    
    private static final char defaultFieldDefDelimiter = ',';
    private static final char defaultNameValueDelimiter = '=';
    private static final char recursiveDefDelimiterStart = '{';
    private static final char recursiveDefDelimiterEnd = '}';
    private static final char typeNameDelimiterStart = '<';
    private static final char typeNameDelimiterEnd = '>';
    
    private static final ConcurrentMap<Class,TemplateAndOptions> templateMap = new ConcurrentHashMap<>();
    
    private static final boolean    debug = false;
    
    private enum NonFatalExceptionResponse {IGNORE_EXCEPTIONS, LOG_EXCEPTIONS, THROW_EXCEPTIONS};
    
    private static void checkFields(Object o, Set<String> fields, String setName) {
        for (String field : fields) {
            try {
                Field   f;
                
                f = o.getClass().getDeclaredField(field);
                if (debug) {
                    System.out.println("optionalField: "+ f);
                }
            } catch (NoSuchFieldException nsfe) {
                throw new RuntimeException(setName +" contained unknown field: "
                            + field +" in "+ o.getClass().getName());
            }
        }
    }
    
    public static void addTemplateAndOptionsAndExcluded(Object o,
                                Set<String> optionalFields, Set<String> excludedFields) {
        Preconditions.checkNotNull(o, "Template object cannot be null");
        // System.out.println("add: "+ o.getClass() +" "+ o);
        if (optionalFields == null) {
            optionalFields = ImmutableSet.of();
        }
        if (excludedFields == null) {
            excludedFields = ImmutableSet.of();
        }
        checkFields(o, optionalFields, "optionalFields");
        checkFields(o, excludedFields, "excludedFields");
        templateMap.put(o.getClass(), new TemplateAndOptions(o, optionalFields,
                excludedFields));
    }
    
    public static void addTemplateAndOptions(Object o, Set<String> optionalFields) {
        addTemplateAndOptionsAndExcluded(o, optionalFields, null);
    }
    
    public static void addTemplateAndExcluded(Object o, Set<String> excludedFields) {
        addTemplateAndOptionsAndExcluded(o, null, excludedFields);
    }
    
    public static void addTemplate(Object o) {
        Set<String> optionalFields;
        
        optionalFields = ImmutableSet.of();
        addTemplateAndOptions(o, optionalFields);
    }
    
    private static <T> T getTemplate(Class _class) {
        TemplateAndOptions   templateAndOptions;
        T   template;
        
        templateAndOptions = templateMap.get(_class);
        if (templateAndOptions != null) {
            template = (T)templateAndOptions.getObj();
            //System.out.println("get: "+ _class +" "+ template);
            return template;
        } else {
            return null;
        }
    }
    
    private ObjectDefParser(T template,
                           FieldsRequirement fieldsRequirement, 
                           NonFatalExceptionResponse nonFatalExceptionResponse,
                           String fieldDefDelimiter, String nameValueDelimiter,
                           Set<String> optionalFields,
                           Set<String> exclusionFields) {
        try {
            this.template = template;
            this.fieldRequirement = fieldsRequirement;
            this.nonFatalExceptionResponse = nonFatalExceptionResponse;
            this.fieldDefDelimiter = fieldDefDelimiter;
            this.nameValueDelimiter = nameValueDelimiter;
            if (optionalFields != null) {
                if (fieldsRequirement == FieldsRequirement.REQUIRE_ALL_FIELDS) {
                    throw new RuntimeException("Optional fields incompatible with FieldsRequirement.REQUIRE_ALL_FIELDS");
                }
                this.optionalFields = optionalFields;
            } else {
                this.optionalFields = ImmutableSet.of();
            }
            if (exclusionFields != null) {
                this.exclusionFields = exclusionFields;
            } else {
                this.exclusionFields = ImmutableSet.of();
            }
            _class = (Class<T>)template.getClass();
            //fields = filterStaticFields(_class.getDeclaredFields());
            fields = filterExcludedFields(filterStaticFields(getDeclaredFields(_class)), this.exclusionFields);
            if (debug) {
                for (Field field : fields) {
                    System.out.println(field.getName());
                }
            }
            //constructor = _class.getConstructor(getFieldClasses(fields));
            constructor = getConstructor(_class, getFieldClasses(fields));
        } catch (NoSuchMethodException nsme) {
            throw new ObjectDefParseException("Can't find constructor to initialize all fields for "
                    + template.getClass().getName() +". "
                    +"Remember order must match", nsme);
        } catch (Exception e) {
            throw new ObjectDefParseException("Error creating template", e);
        }
    }
    
    private Field[] getDeclaredFields(Class _class) {
        return getDeclaredFieldsList(_class).toArray(new Field[0]);
    }
    
    private List<Field> getDeclaredFieldsList(Class _class) {
        List<Field> fields;
        Class   superClass;
        
        fields = new ArrayList<>();
        fields.addAll(Arrays.asList(_class.getDeclaredFields()));
        superClass = _class.getSuperclass();
        if (superClass != Object.class) {
            List<Field> superFields;

            superFields = getDeclaredFieldsList(superClass);
            superFields.addAll(fields);
            return superFields;
        } else {
            return fields;
        }
    }
    
    private Constructor<T> getConstructor(Class _class, Class[] fields) throws NoSuchMethodException {
        Constructor<T>[]    constructors;
        
        constructors = _class.getConstructors();
        for (Constructor<T> constructor : constructors) {
            if (constructorMatches(constructor, fields)) {
                return constructor;
            }
        }
        throw new NoSuchMethodException();
    }
    
    private boolean constructorMatches(Constructor<T> constructor, Class[] fields) {
        Class[] cFields;
     
        cFields = constructor.getParameterTypes();
        if (debug) {
            System.out.printf("\ncFields\n%s\nfields\n%s\n\n", StringUtil.arrayToString(cFields), StringUtil.arrayToString(fields));
        }
        if (fields.length != cFields.length) {
            if (debug) {
                System.out.printf("Returning false.....................\n");
            }
            return false;
        } else {
            for (int i = 0; i < fields.length; i++) {
                if (!cFields[i].isAssignableFrom(fields[i])) {
                    if (debug) {
                        System.out.printf("%s isInstance %s failed\n", cFields[i], fields[i]);
                    }
                    if (debug) {
                        System.out.printf("Returning false.....................\n");
                    }
                    return false;
                }
            }
            if (debug) {
                System.out.printf("Returning true.....................\n");
            }
            return true;
        }
    }

    public ObjectDefParser(T template, FieldsRequirement fieldsRequirement, Set<String> optionalFields,
                           Set<String> exclusionFields) {
        this(template, 
                fieldsRequirement, 
                NonFatalExceptionResponse.THROW_EXCEPTIONS,
                //NonFatalExceptionResponse.IGNORE_EXCEPTIONS,
                Character.toString(defaultFieldDefDelimiter), 
                Character.toString(defaultNameValueDelimiter),
                optionalFields,
                exclusionFields);
    }
    
    public ObjectDefParser(T template, FieldsRequirement fieldsRequirement, Set<String> optionalFields) {
        this(template, fieldsRequirement, optionalFields, null);
    }
    
    public ObjectDefParser(T template, FieldsRequirement fieldsRequirement) {
        this(template, fieldsRequirement, null);
    }
    
    public ObjectDefParser(T template) {
        this(template, FieldsRequirement.ALLOW_INCOMPLETE);
    }
    
    public ObjectDefParser(T template, Set<String> exclusionFields) {
        this(template, FieldsRequirement.ALLOW_INCOMPLETE, null, exclusionFields);
    }
    
    public ObjectDefParser(Class<T> _class, FieldsRequirement fieldsRequirement, Set<String> optionalFields) {
        this((T)getTemplate(_class), fieldsRequirement, optionalFields);
    }
    
    public ObjectDefParser(Class<T> _class) {
        this((T)getTemplate(_class));
    }
    
    private static Field[] filterStaticFields(Field[] fields) {
        List<Field> nonStatic;
        
        nonStatic = new ArrayList<>();
        for (Field field : fields) {
            if ((field.getModifiers() & Modifier.STATIC) == 0) {
                nonStatic.add(field);
            }
        }
        return nonStatic.toArray(new Field[0]);
    }
    
    private static Field[] filterExcludedFields(Field[] fields, Set<String> exclusionSet) {
        List<Field> passed;
        
        passed = new ArrayList<>();
        for (Field field : fields) {
            if (!exclusionSet.contains(field.getName())) {
                passed.add(field);
            }
        }
        return passed.toArray(new Field[0]);
    }
    
    private Class<?>[] getFieldClasses(Field[] fields) {
        Class<?>[] classes;
        
        classes = new Class[fields.length];
        for (int i = 0; i < fields.length; i++) {
            classes[i] = fields[i].getType();
        }
        if (debug) {
            System.out.println(".....");
            for (Class c : classes) {
                System.out.println(c);
            }
        }
        return classes;
    }
    
    private Object[] createConstructorArgs(Map<String,String> defMap) {
        Object[]    args;
        
        args = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            String  name;
            String  value;
            
            name = fields[i].getName();
            value = defMap.get(name);
            if (value == null) {
                if (fieldRequirement == FieldsRequirement.REQUIRE_ALL_FIELDS) {
                    throw new ObjectDefParseException("Missing required field: "+ name);
                } else if (fieldRequirement == FieldsRequirement.REQUIRE_ALL_NONOPTIONAL_FIELDS 
                            && !optionalFields.contains(name)) {
                    throw new ObjectDefParseException("Missing required field: "+ name);
                } else {
                    try {
                        fields[i].setAccessible(true);
                        args[i] = fields[i].get(template);
                    } catch (Exception e) {
                        throw new ObjectDefParseException("Unable to set field value from template", e);
                    }
                }
            } else {
                try {
                    args[i] = valueForDef(fields[i].getType(), value);
                } catch (ObjectDefParseException e) {
                    if (nonFatalExceptionResponse == NonFatalExceptionResponse.LOG_EXCEPTIONS) {
                        Log.logErrorWarning(e, "Logging and ignoring");
                    }
                    if (nonFatalExceptionResponse != NonFatalExceptionResponse.THROW_EXCEPTIONS) {
                        try {
                            fields[i].setAccessible(true);
                            args[i] = fields[i].get(template);
                        } catch (Exception e2) {
                            throw new ObjectDefParseException("Unable to set field value from template", e2);
                        }
                    } else {
                        throw e;
                    }
                }
            }
            if (debug && false) {
                System.out.printf("%s\t%s\n", fields[i].getName(), args[i]);
            }
        }
        return args;
    }
    
    private Object valueForDef(Class type, String def) {
        if (debug) {
            System.out.printf("valueForDef %s %s %s\n", type, def, type == long.class);
        }
        if (type.isEnum()) {
            try {
                Method  valueOf;
                
                valueOf = type.getMethod("valueOf", String.class);
                return valueOf.invoke(null, def);
            } catch (Exception e) {
                throw new ObjectDefParseException("Unable to create enum", e);
            }
        } else {
            // FUTURE - consider invoking a generic string-supporting constructor
            if (type == Byte.class || type == byte.class) {
                return Byte.parseByte(def);
            } else if (type == Character.class || type == char.class) {
                return new Character(def.charAt(0));
            } else if (type == Short.class || type == short.class) {
                return Short.parseShort(def);
            } else if (type == Integer.class || type == int.class) {
                return Integer.parseInt(def);
            } else if (type == Long.class || type == long.class) {
                return Long.parseLong(def);
            } else if (type == Float.class || type == float.class) {
                return Float.parseFloat(def);
            } else if (type == Double.class || type == double.class) {
                return Double.parseDouble(def);
            } else if (type == String.class) {
                return def;
            } else if (type == Boolean.class || type == boolean.class) {
                return Boolean.parseBoolean(def);
            } else if (type == Map.class) {
                return parseMap(def);
            } else if (type == Set.class) {
                return parseSet(def);
            } else {
                TemplateAndOptions  subTemplateAndOptions;
                
                if (def.startsWith(Character.toString(typeNameDelimiterStart))) {
                    int typeNameEnd;
                    
                    typeNameEnd = def.indexOf(typeNameDelimiterEnd);
                    if (typeNameEnd < 0) {
                        Log.warning("type: "+ type);
                        Log.warning("def: "+ def);
                        throw new ObjectDefParseException("\n"+ type +" Missing typeNameDelimiterEnd "+ def);
                    } else if (typeNameEnd >= def.length() - 1) {
                            Log.warning("type: "+ type);
                            Log.warning("def: "+ def);
                            throw new ObjectDefParseException("\n"+ type +" Found type, missing def "+ def);
                    } else {
                        String  typeName;
                        
                        typeName = def.substring(1, typeNameEnd);
                        def = def.substring(typeNameEnd + 1);
                        if (typeName.indexOf('.') < 0) {
                            typeName = type.getPackage().getName() +"."+ typeName;
                        }
                        try {
                            type = Class.forName(typeName);
                        } catch (ClassNotFoundException cnfe) {
                            throw new ObjectDefParseException(cnfe);
                        }
                    }
                }
                
                subTemplateAndOptions = templateMap.get(type);
                if (subTemplateAndOptions != null) {                    
                    if (def.startsWith(Character.toString(recursiveDefDelimiterStart)) 
                            && def.endsWith(Character.toString(recursiveDefDelimiterEnd))) {
                        Object  value;
                        
                        def = def.substring(1, def.length() - 1);
                        if (debug) {
                            System.out.println("ObjectDefParser.valueForDef() recursive for: "+ def);
                        }
                        //System.out.println("\n\n"+ def);
                        //value = new ObjectDefParser(subTemplateAndOptions.getObj(), fieldRequirement, nonFatalExceptionResponse, 
                        //            def, def, subTemplateAndOptions.getOptionalFields()).parse(def);
                        
                        Method  parseMethod;
                        
                        try {
                            parseMethod = subTemplateAndOptions.getObj().getClass().getMethod("parse", String.class);
                            value = parseMethod.invoke(subTemplateAndOptions, def);
                        } catch (NoSuchMethodException e) {
                            throw new RuntimeException(e);
                        } catch (SecurityException e) {
                            throw new RuntimeException(e);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        } catch (IllegalArgumentException e) {
                            throw new RuntimeException(e);
                        } catch (InvocationTargetException e) {
                            throw new RuntimeException(e);
                        }
                        
                        
                        if (debug) {
                            System.out.println("recursive value: "+ value);
                            System.out.println("ObjectDefParser.valueForDef() complete recursive for: "+ def);
                        }
                        return value;
                    } else {
                        throw new ObjectDefParseException("sub def not delimited: "+ def);
                    }
                } else {
                    Log.warning("type: "+ type);
                    Log.warning("def: "+ def);
                    throw new ObjectDefParseException("\n"+ type +"Unsupported field type: "+ type);
                }
            }
        }
    }
    
    public T parse(String fieldDefs) {
        Map<String,String>  defMap;
        
        if (debug) {
            System.out.println("parse "+ fieldDefs);
        }
        defMap = new HashMap<>();
        for (String fieldDef : splitFieldDefs(fieldDefs)) {
            String[]    nameAndValue;
            String      fieldName;
            String      valueDef;
            
            nameAndValue = splitNameAndValue(fieldDef);
            if (nameAndValue.length != 2) {
                throw new ObjectDefParseException("bad nameAndValue: "+ fieldDef);
            }
            fieldName = nameAndValue[0];
            valueDef = nameAndValue[1];
            if (debug) {
                System.out.println("\t\t"+ fieldName +"\t"+ valueDef);
            }
            defMap.put(fieldName, valueDef);
        }
        try {
            if (debug) {
                System.out.printf(">>>%s\n", defMap.get("version"));
                System.out.println("::::"+ constructor.newInstance(createConstructorArgs(defMap)));
            }
            return constructor.newInstance(createConstructorArgs(defMap));
        } catch (Exception e) {
            throw new ObjectDefParseException("Exception creating instance", e);
        }
    }
    
    private String[] splitNameAndValue(String fieldDef) {
        int i;
        
        i = fieldDef.indexOf(defaultNameValueDelimiter);
        if (i < 0) {
            return new String[0];
        } else {
            String[]    s;
            
            s = new String[2];
            s[0] = fieldDef.substring(0, i);
            s[1] = fieldDef.substring(i + 1);
            return s;
        }
    }

    private String[] splitFieldDefs(String fieldDefs) {
        //return fieldDefs.split(fieldDefDelimiter);
        List<String>    defs;
        int             depth;
        int             last;

        last = 0;
        depth = 0;
        defs = new ArrayList<>();
        for (int i = 0; i < fieldDefs.length(); i++) {
            switch(fieldDefs.charAt(i)) {
            case defaultFieldDefDelimiter:
                if (depth == 0) {
                    defs.add(fieldDefs.substring(last, i));
                    last = i + 1;
                }
                break;
            case recursiveDefDelimiterStart: 
                depth++; 
                break;
            case recursiveDefDelimiterEnd: 
                depth--; 
                break;
            }
        }
        if (last < fieldDefs.length()) {
            defs.add(fieldDefs.substring(last, fieldDefs.length()));
        }
        return defs.toArray(new String[0]);
    }

    public String objectToString(T obj) {
        /*
        Class   superClass;
        
        System.out.printf("************************************************\n");
        System.out.printf("************************************************\n");
        System.out.printf("************************************************\n");
        System.out.printf("************************************************\n");
        System.out.printf("************************************************\n");
        System.out.printf("************************************************\n");
        superClass = _class.getSuperclass();
        System.out.printf("superClass %s\n", superClass);
        if (superClass != Object.class) {
            return _objectToString(superClass, obj) + " **** " + _objectToString(_class, obj);
            //return _objectToString(superClass, obj) + fieldDefDelimiter + _objectToString(_class, obj);
        } else {
            return _objectToString(_class, obj);
        }
        */
        return _objectToString(_class, obj);
    }
    
    private String _objectToString(Class _c, T obj) {
        Class   superClass;
        String  superClassString;
        StringBuilder   sb;
        Field[] fields;
        
        sb = new StringBuilder();
        superClass = _c.getSuperclass();
        if (superClass != Object.class) {
            superClassString = _objectToString(superClass, obj);
            sb.append(superClassString);
            sb.append(fieldDefDelimiter);
        }
        fields = filterExcludedFields(filterStaticFields(_c.getDeclaredFields()), exclusionFields);
        for (int i = 0; i < fields.length; i++) {
            Field   field;
            boolean recursive;
            String  valueString;
            String  typeNameString;
            
            valueString = null;
            typeNameString = null;
            field = fields[i];
            if (field.getType() == Set.class) {
                field.setAccessible(true);
                try {
                    if (field.get(obj) == null) {
                        valueString = "<error null>";
                    } else {
                        valueString = CollectionUtil.toString((Set)field.get(obj), ',');
                    }
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                recursive = false;
            } else {
                if (field.getType().isInterface() && !isSpecialCase(field.getType())) {
                    String  ns;
                    Class   actualFieldType;
                    
                    recursive = true;
                    try {
                        field.setAccessible(true);
                        actualFieldType = field.get(obj).getClass();
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    if (obj.getClass().getPackage() == _c.getPackage()) {
                        ns = actualFieldType.getSimpleName();
                    } else {
                        ns = actualFieldType.getName();
                    }
                    typeNameString = typeNameDelimiterStart + ns + typeNameDelimiterEnd;
                } else {
                    recursive = templateMap.containsKey(field.getType());
                }
                
                if (debug) {
                    System.out.printf("%s\trecursive %s\n", field.getName(), recursive);
                }
                try {
                    Object  value;
                    
                    value = field.get(obj);
                    //if (value != null || !optionalFields.contains(field.getName())) {
                    if (value != null) {
                        valueString = value.toString();
                    }
                } catch (IllegalAccessException e) {
                    try {
                        Object  value;
                        
                        field.setAccessible(true);
                        value = field.get(obj);
                        if (value != null) {
                            valueString = value.toString();
                        }
                    } catch (Exception e2) {
                        throw new RuntimeException(e2);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            
            if (valueString != null) {
                sb.append(field.getName());
                sb.append(nameValueDelimiter);
                if (recursive) {
                    if (typeNameString != null) {
                        sb.append(typeNameString);
                    }
                    sb.append(recursiveDefDelimiterStart);
                }
                sb.append(valueString);
                if (recursive) {
                    sb.append(recursiveDefDelimiterEnd);
                }
                if (i < fields.length - 1) {
                    sb.append(fieldDefDelimiter);
                }
            }
        }
        return sb.toString();
    }
    
    private boolean isSpecialCase(Class<?> type) {
        return type == Map.class || type == Set.class;
    }

    private Map<String,String> parseMap(String def) {
        try {
            def=def.trim();
            if (!def.startsWith(""+ recursiveDefDelimiterStart)) {
                throw new RuntimeException("Bad map def: "+ def);
            }
            if (!def.endsWith(""+ recursiveDefDelimiterEnd)) {
                throw new RuntimeException("Bad map def: "+ def);
            }
            def = def.substring(1, def.length() - 1);
            if (def.indexOf(',') >= 0) {
                def = def.replace(',', '\n');
            }
            return StreamParser.parseMap(new ByteArrayInputStream(def.getBytes()));
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    private Set<String> parseSet(String def) {
        try {
            def=def.trim();
            if (!def.startsWith(""+ recursiveDefDelimiterStart)) {
                throw new RuntimeException("Bad map def: "+ def);
            }
            if (!def.endsWith(""+ recursiveDefDelimiterEnd)) {
                throw new RuntimeException("Bad map def: "+ def);
            }
            def = def.substring(1, def.length() - 1);
            return StreamParser.parseSet(new ByteArrayInputStream(def.getBytes()));
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    private static class TemplateAndOptions {
        private final Object        obj;
        private final Set<String>   optionalFields;
        private final Set<String>   excludedFields;
        
        TemplateAndOptions(Object obj, Set<String> optionalFields, Set<String> excludedFields) {
            this.obj = obj;
            this.optionalFields = optionalFields;
            this.excludedFields = excludedFields;
        }
        
        Object getObj() {
            return obj;
        }

        Set<String> getOptionalFields() {
            return optionalFields;
        }
        
        Set<String> getExcludedFields() {
            return excludedFields;
        }
    }
}
