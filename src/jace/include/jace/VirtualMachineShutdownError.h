#ifndef JACE_VIRTUAL_MACHINE_SHUTDOWN_ERROR_H
#define JACE_VIRTUAL_MACHINE_SHUTDOWN_ERROR_H

#include "jace/JNIException.h"

BEGIN_NAMESPACE(jace)


/**
 * An operation has failed because the virtual machine has shut down.
 *
 * @author Toby Reyelts
 */
class VirtualMachineShutdownError: public ::jace::JNIException
{
public:
	/**
	 * Creates a new VirtualMachineShutdownError with the given message.
	 */
	VirtualMachineShutdownError(const std::string& value) throw ():
			JNIException(value)
	{}
};

END_NAMESPACE(jace)

#endif // #ifndef JACE_VIRTUAL_MACHINE_SHUTDOWN_ERROR_H

