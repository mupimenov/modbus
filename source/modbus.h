/*
COPYRIGHT (c) 2017 Mikhail Pimenov
MIT License
Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:
The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#ifndef __MODBUS_H_
#define __MODBUS_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum modbus_request_function {
	MODBUS_READ_COILS                	= 0x01,
	MODBUS_READ_DISCRETE_INPUTS      	= 0x02,
	MODBUS_READ_HOLDING_REGISTERS    	= 0x03,
	MODBUS_READ_INPUT_REGISTERS      	= 0x04,
	MODBUS_WRITE_SINGLE_COIL         	= 0x05,
	MODBUS_WRITE_SINGLE_REGISTER     	= 0x06,
	MODBUS_READ_EXCEPTION_STATUS     	= 0x07,
	MODBUS_WRITE_MULTIPLE_COILS      	= 0x0F,
	MODBUS_WRITE_MULTIPLE_REGISTERS 	= 0x10,
	MODBUS_REPORT_SLAVE_ID           	= 0x11,
	MODBUS_READ_GENERAL_REFERENCE 		= 0x14,
	MODBUS_WRITE_GENERAL_REFERENCE 		= 0x15,
	MODBUS_MASK_WRITE_REGISTER 			= 0x16,
	MODBUS_WRITE_AND_READ_REGISTERS  	= 0x17
};

struct modbus_request {
	uint8_t address;
	enum modbus_request_function function;
	const uint8_t *data;
	uint16_t dlen;
};

enum modbus_answer_error {
	MODBUS_ERROR_OK 					= 0x00,

	MODBUS_ERROR_ILLEGAL_FUNCTION 		= 0x01,
	MODBUS_ERROR_ILLEGAL_DATA_ADDRESS 	= 0x02,
	MODBUS_ERROR_ILLEGAL_DATA_VALUE 	= 0x03,
	MODBUS_ERROR_SERVICE_DEVICE_FAILURE = 0x04,
	MODBUS_ERROR_ACKNOWLEDGE 			= 0x05,
	MODBUS_ERROR_SERVER_DEVICE_BUSY 	= 0x06,
	MODBUS_ERROR_MEMORY_PARITY_ERROR 	= 0x08
};

enum modbus_answer_data_type {
	MODBUS_DATA_TYPE_RAW,
	MODBUS_DATA_TYPE_BIT,
	MODBUS_DATA_TYPE_HALF_WORD
};

struct modbus_answer {
	enum modbus_answer_error error;
	enum modbus_answer_data_type dtype;
	uint8_t *data;
	uint8_t dlen;
};

enum modbus_table {
	MODBUS_TABLE_COILS,
	MODBUS_TABLE_DISCRETE_INPUTS,
	MODBUS_TABLE_HOLDING_REGISTERS,
	MODBUS_TABLE_INPUT_REGISTERS
};

enum modbus_lock_type {
	MODBUS_LOCK,
	MODBUS_UNLOCK
};

struct modbus_bits_table {
	uint16_t address;
	uint16_t count;
	uint8_t *bits;
};

struct modbus_regs_table {
	uint16_t address;
	uint16_t count;
	uint16_t *regs;
};

struct modbus_functions;

enum modbus_transport {
	MODBUS_RTU,
	MODBUS_TCP
};

struct modbus_instance {
	// data
	void *arg;
	uint8_t *recv_buffer;
	uint8_t *send_buffer;
	uint8_t recv_buffer_size;
	uint8_t send_buffer_size;

	// tables
	const struct modbus_bits_table *coil_tables;
	const struct modbus_bits_table *discrete_tables;
	const struct modbus_regs_table *input_tables;
	const struct modbus_regs_table *holding_tables;

	// functions
	const struct modbus_functions *functions;

	// id
	const char *id;
	// transport
	enum modbus_transport transport;
	// address
	uint8_t address;
	// status
	uint32_t error;
};

struct modbus_functions {
	// io
	int (*open)(struct modbus_instance *instance);
	int (*write)(struct modbus_instance *instance, const uint8_t *packet, uint8_t plen);
	int (*read)(struct modbus_instance *instance, uint8_t *buffer, uint8_t max_size);
	int (*close)(struct modbus_instance *instance);

	// locks
	int (*lock)(struct modbus_instance *instance, enum modbus_table table, enum modbus_lock_type lock_);

	// hooks
	int (*before_read_table)(struct modbus_instance *instance, enum modbus_table table, uint16_t address, uint16_t count);
	int (*after_write_table)(struct modbus_instance *instance, enum modbus_table table, uint16_t address, uint16_t count);

	// files
	int (*read_file)(struct modbus_instance *instance, uint16_t filenum, uint16_t address, uint16_t count, uint8_t *data);
	int (*write_file)(struct modbus_instance *instance, uint16_t filenum, uint16_t address, uint16_t count, const uint8_t *data);
};

enum modbus_instance_error {
	MODBUS_SUCCESS = 0,
	MODBUS_INVAL,
	MODBUS_DEVICE_ERROR,
	MODBUS_NO_PACKET,
	MODBUS_NOT_IMPLEMENTED,
	MODBUS_BAD_CRC,
	MODBUS_BAD_ADDRESS,
	MODBUS_BAD_COMMAND,
	MODBUS_BAD_PARAMS,
	MODBUS_INT
};

#define MODBUS_RETURN(__instance__, __err__) { (__instance__)->error = __err__; return (__err__? -1: 0); }

int modbus_io(struct modbus_instance *instance);

#ifdef __cplusplus
}
#endif

#endif /* __MODBUS_H_ */
