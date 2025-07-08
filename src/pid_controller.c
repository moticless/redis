#include "pid_controller.h"
#include <stdlib.h>
#include <float.h>

PidController* pid_create(double kp, double ki, double kd,
                          double output, double min_output, double max_output) {
    PidController *controller = (PidController *)malloc(sizeof(PidController));
    if (controller) {
        controller->kp = kp;
        controller->ki = ki;
        controller->kd = kd;
        controller->integral = 0.0;
        controller->previous_error = 0.0;
        controller->output = output;
        controller->min_output = min_output;
        controller->max_output = max_output;
    }
    return controller;
}

double pid_update(PidController *controller, double setpoint, double measurement) {
    double error = setpoint - measurement;

    controller->integral += error;
    double derivative = error - controller->previous_error;

    double adjustment = (controller->kp * error) +
                        (controller->ki * controller->integral) +
                        (controller->kd * derivative);

    controller->previous_error = error;

    controller->output += adjustment;

    if (controller->output < controller->min_output)
        controller->output = controller->min_output;
    else if (controller->output > controller->max_output)
        controller->output = controller->max_output;

    return controller->output;
}

void pid_reset(PidController *controller) {
    controller->integral = 0.0;
    controller->previous_error = 0.0;
    controller->output = controller->min_output;
}

void pid_destroy(PidController *controller) {
    if (controller)
        free(controller);
}
