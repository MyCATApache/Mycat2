package io.mycat.ui;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Set;
import java.util.stream.Collectors;

public interface VO {

    String toJsonConfig();

    void from(String text);

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();

   default Validator getValidator(){
      return factory.getValidator();
    }
    default <T>  T validate(T t) {
        Set<ConstraintViolation<T>> validate = getValidator().validate(t);
        if (!validate.isEmpty()){
            throw new IllegalArgumentException(validate.stream().map(i -> i.getMessage()).collect(Collectors.joining("\n")));
        }
        return t;
    }
}
