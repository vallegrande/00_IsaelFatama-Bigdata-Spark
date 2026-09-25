package pe.edu.vallegrande.bigdata.contracts;

public enum QualityRule {
    CAMPO_REQUERIDO("COMPLETITUD", "Falta un valor obligatorio"),

    NOTA_NO_NUMERICA("VALIDEZ", "La nota no es un número"),

    NOTA_FUERA_DE_RANGO("VALIDEZ", "La nota esta fuera de la escala 0 a 20"),

    PESO_INVALIDO("VALIDEZ", "El peso de la evaluacion debe de ser mayor a 0 y menor o igual a 1"),

    ESTADO_INVALIDO("VALIDEZ", "El estado de asistencia debe de ser P, T, F o J"),

    FECHA_INVALIDA("VALIDEZ", "La fecha no tiene el formato AAAA-MM-DD válido"),

    NUMERO_INVALIDO("VALIDEZ", "Un valor número entero no es válido o no es positivo"),

    CORREO_INVALIDO("VALIDEZ", "El correo no es correcto"),

    REGISTRO_DUPLICADO("UNICIDAD", "La clave del registro ya existe"),

    SEMESTRE_INEXISTENTE("INTEGRIDAD", "El semestre no existe en la tabla de semestres"),

    ESTUDIANTE_INEXISTENTE("INTEGRIDAD", "El estudiante no existe en el registro"),

    CURSO_INEXISTENTE("INTEGRIDAD", "El curso no existe en el registro"),

    CURSO_DE_OTRO_SEMESTRE("CONSISTENCIA", "El curso no pertenece al semestre del estudiante"),

    PERIODO_INCONSISTENTE("CONSISTENCIA", "El periodo no coincide con el semestre del curso"),

    FECHA_FUERA_DE_PERIODO("CONSISTENCIA", "La fechja esta fuera del inicio y fin del semestre");


    private final String dimension;

    private final String description;

    QualityRule(String dimension, String description) {
        this.dimension = dimension;
        this.description = description;
    }

    public String dimension(){
        return dimension;
    }

    public String description(){
        return description;
    }
}
