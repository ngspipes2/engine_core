package pt.isel.ngspipes.engine_core.utils;

import pt.isel.ngspipes.engine_core.exception.InputValidationException;

import java.io.File;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

public class TypeValidator {

    @FunctionalInterface
    public interface IValidator {

        <T> void validate(T value) throws InputValidationException;

    }

    public static final Map<String, IValidator> VALIDATORS;

    static {
        VALIDATORS = new HashMap<>();
        VALIDATORS.put("string", TypeValidator::validateString);
        VALIDATORS.put("int", TypeValidator::validateInt);
        VALIDATORS.put("float", TypeValidator::validateFloat);
        VALIDATORS.put("double", TypeValidator::validateDouble);
        VALIDATORS.put("flag", TypeValidator::validateFlag);
        VALIDATORS.put("file", TypeValidator::validateFile);
        VALIDATORS.put("directory", TypeValidator::validateDirectory);
        VALIDATORS.put("compose", TypeValidator::validateDummy);
        VALIDATORS.put("enum", TypeValidator::validateDummy);
        VALIDATORS.put("string[]", TypeValidator::validateStringArray);
        VALIDATORS.put("int[]", TypeValidator::validateIntArray);
        VALIDATORS.put("float[]", TypeValidator::validateFloatArray);
        VALIDATORS.put("double[]", TypeValidator::validateDoubleArray);
        VALIDATORS.put("file[]", TypeValidator::validateFileArray);
        VALIDATORS.put("directory[]", TypeValidator::validateDirectoryArray);
        VALIDATORS.put("string[][]", TypeValidator::validateStringArrayOfArray);
        VALIDATORS.put("int[][]", TypeValidator::validateIntArrayOfArray);
        VALIDATORS.put("float[][]", TypeValidator::validateFloatArrayOfArray);
        VALIDATORS.put("double[][]", TypeValidator::validateDoubleArrayOfArray);
        VALIDATORS.put("file[][]", TypeValidator::validateFileArrayOfArray);
        VALIDATORS.put("directory[][]", TypeValidator::validateDirectoryArrayOfArray);
    }


    /* IValidator Implementations */
    private static void validateInt(Object value) throws InputValidationException {
        try {
            Integer.parseInt(value.toString());
        } catch (NumberFormatException ex) {
            throw new InputValidationException("Value " +  value + " is not a valid int. Value must be between: "
                                                + Integer.MIN_VALUE + " AND " + Integer.MAX_VALUE + ".", ex);
        }
    }

    private static void validateString(Object value) throws InputValidationException {
        if (!(value instanceof String))
            throw new InputValidationException("Value " +  value + " is not a valid string.");
    }

    private static void validateDouble(Object value) throws InputValidationException {
        try {
           Double.parseDouble(value.toString());
        } catch (NumberFormatException ex) {
            throw new InputValidationException("Value " +  value + " is not a valid double. Value must be between: "
                                                + Double.MIN_VALUE + " AND " + Double.MAX_VALUE + ".",ex);
        }
    }

    private static void validateFlag(Object value) throws InputValidationException {
        if (!(value instanceof Boolean))
            throw new InputValidationException("Value " +  value + " is not a valid boolean. Value must be: "
                                                + Boolean.FALSE + " OR " + Boolean.TRUE + ".");
    }

    private static void validateFloat(Object value) throws InputValidationException {
        try {
            Float.parseFloat(value.toString());
        } catch (NumberFormatException ex) {
            throw new InputValidationException("Value " +  value + " is not a valid float. Value must be between: "
                                                 + Float.MIN_VALUE + " AND " + Float.MAX_VALUE + ".",ex);
        }
    }

    private static void validateDirectory(Object value) throws InputValidationException {
        File file = new File(value.toString());
        try {
            boolean exist = file.exists() && file.isDirectory();
        } catch (NullPointerException ex) {
            throw new InputValidationException("Value " +  value + " is not a valid path for a directory.", ex);
        }
    }

    private static void validateDummy(Object value) { }

    private static void validateFile(Object value) throws InputValidationException {
        File file = new File(value.toString());
        try {
            boolean exist = file.exists() && file.isFile();
        } catch (NullPointerException ex) {
            throw new InputValidationException("Value " +  value + " is not a valid path for a file.", ex);
        }
    }

    private static void validateStringArray(Object value) throws InputValidationException {
        IValidator validator = TypeValidator::validateString;
        validateArray(value, validator);
    }

    private static void validateIntArray(Object value) throws InputValidationException {
        IValidator validator  = TypeValidator::validateInt;
        validateArray(value, validator);
    }

    private static void validateFloatArray(Object value) throws InputValidationException {
        IValidator validator  = TypeValidator::validateFloat;
        validateArray(value, validator);
    }

    private static void validateDoubleArray(Object value) throws InputValidationException {
        IValidator validator  = TypeValidator::validateDouble;
        validateArray(value, validator);
    }

    private static void validateFileArray(Object value) throws InputValidationException {
        IValidator validator  = TypeValidator::validateFile;
        validateArray(value, validator);
    }

    private static void validateDirectoryArray(Object value) throws InputValidationException {
        IValidator validator  = TypeValidator::validateDirectory;
        validateArray(value, validator);
    }

    private static void validateStringArrayOfArray(Object value) throws InputValidationException {
        IValidator validator = TypeValidator::validateString;
        validateArrayOfArray(value, validator);
    }

    private static void validateIntArrayOfArray(Object value) throws InputValidationException {
        IValidator validator = TypeValidator::validateInt;
        validateArrayOfArray(value, validator);
    }

    private static void validateFloatArrayOfArray(Object value) throws InputValidationException {
        IValidator validator = TypeValidator::validateFloat;
        validateArrayOfArray(value, validator);
    }

    private static void validateDoubleArrayOfArray(Object value) throws InputValidationException {
        IValidator validator = TypeValidator::validateDouble;
        validateArrayOfArray(value, validator);
    }

    private static void validateFileArrayOfArray(Object value) throws InputValidationException {
        IValidator validator = TypeValidator::validateFile;
        validateArrayOfArray(value, validator);
    }

    private static void validateDirectoryArrayOfArray(Object value) throws InputValidationException {
        IValidator validator = TypeValidator::validateDirectory;
        validateArrayOfArray(value, validator);
    }

    private static void validateArrayOfArray(Object value, IValidator validator) throws InputValidationException {
        String[] arrays = getArrays(value);
        for (String array : arrays)
            validateArray(array, validator);
    }

    private static String[] getArrays(Object value) throws InputValidationException {
        if (value instanceof String) {
            String valueStr = getStringWithoutBrackets((String) value);
            return valueStr.split("]\\s*,\\s*\\[");
        }
        throw new InputValidationException("Value " +  value + " is not a valid string.");
    }

    private static void validateArray(Object value, IValidator validator) throws InputValidationException {
        String[] array = getArray(value);
        for (String currVaslue : array) {
            currVaslue = currVaslue.trim();
            chekIfIs(currVaslue, validator);
        }
    }

    private static String[] getArray(Object value) throws InputValidationException {
        if (value instanceof String) {
            String valueStr = getStringWithoutBrackets((String) value);
            return valueStr.split(",");
        }
        throw new InputValidationException("Value " +  value + " is not a valid string.");
    }

    private static String getStringWithoutBrackets(String value) {
        String valueStr = value.trim();
        int idxLeftBracket = valueStr.indexOf("[");
        int idxRightBracket = valueStr.lastIndexOf("]");
        if(idxLeftBracket >= 0 && idxRightBracket >= 0)
            return valueStr.substring(idxLeftBracket + 1, idxRightBracket);
        if (idxLeftBracket >= 0)
            return valueStr.substring(idxLeftBracket + 1);
        if (idxRightBracket >= 0)
            return valueStr.substring(0 , idxRightBracket);
        return valueStr;
    }

    private static void chekIfIs(String value, IValidator validator) throws InputValidationException {
        validator.validate(value);
    }
}
