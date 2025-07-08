#ifndef PID_CONTROLLER_H
#define PID_CONTROLLER_H

#include <stdint.h>

typedef struct {
    double kp;
    double ki;
    double kd;
    double integral;
    double previous_error;
    double output;
    double min_output;
    double max_output;
} PidController;

/**
 * Create and initialize a PID controller.
 *
 * @param kp         - Proportional gain
 * @param ki         - Integral gain
 * @param kd         - Derivative gain
 * @param output     - Initial output value
 * @param min_output - Minimum allowed output
 * @param max_output - Maximum allowed output
 * @return           - Pointer to the new PID controller
 */
PidController* pid_create(double kp, double ki, double kd,
                          double output, double min_output, double max_output);

/**
 * Compute the new control output using the PID algorithm.
 *
 * @param controller     - PID controller instance
 * @param setpoint       - Desired target value
 * @param measurement    - Current measured value
 * @return               - New output value
 */
double pid_update(PidController *controller, double setpoint, double measurement);

/**
 * Reset the internal state of the PID controller.
 *
 * @param controller - PID controller to reset
 */
void pid_reset(PidController *controller);

/**
 * Free the PID controller instance.
 *
 * @param controller - PID controller to destroy
 */
void pid_destroy(PidController *controller);

#endif // PID_CONTROLLER_H
