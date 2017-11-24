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
#include "modbus.h"

#include <string.h>

#define IS_BROADCAST(__request__) ((__request__)->address == 0? 1u: 0u)

/* STATIC */

static uint16_t crc16(const uint8_t *buffer, uint16_t buffer_length);

/* MODBUS_ERROR */

static int modbus_send_error(struct modbus_instance *instance, uint8_t function, uint8_t error, uint8_t broadcast);

/* MODBUS_READ_* */

typedef struct modbus_read_command {
  uint16_t address;
  uint16_t count;
} modbus_read_command_t;

static int modbus_parse_read_command(const uint8_t *data, uint8_t dlen, struct modbus_read_command *command);

/* MODBUS_READ_COILS */

static int modbus_read_coils_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_READ_DISCRETE_INPUTS */

static int modbus_read_discrete_inputs_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_READ_HOLDING_REGISTERS */

static int modbus_read_holding_registers_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_READ_INPUT_REGISTERS */

static int modbus_read_input_registers_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_WRITE_BIT */

typedef struct modbus_write_bit_command {
  uint16_t address;
  uint16_t data;
} modbus_write_bit_command_t;

static int modbus_parse_write_bit_command(const uint8_t *data, uint8_t dlen, struct modbus_write_bit_command *command);

/* MODBUS_WRITE_REG */

typedef struct modbus_write_reg_command {
  uint16_t address;
  uint16_t data;
} modbus_write_reg_command_t;

static int modbus_parse_write_reg_command(const uint8_t *data, uint8_t dlen, struct modbus_write_reg_command *command);

/* MODBUS_WRITE_SINGLE_COIL */

static int modbus_write_coil_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_WRITE_SINGLE_REGISTER */

static int modbus_write_register_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_WRITE_MULTIPLE_* */

typedef struct modbus_write_multiple_command {
  uint16_t address;
  uint16_t count;
  uint8_t bytes;
} modbus_write_multiple_command_t;

static int modbus_parse_write_multiple_command(const uint8_t *data, uint8_t dlen, struct modbus_write_multiple_command *command);

/* MODBUS_WRITE_MULTIPLE_COILS */

static int modbus_write_multiple_coils_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_WRITE_MULTIPLE_REGISTERS */

static int modbus_write_multiple_regs_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_REPORT_SLAVE_ID */

static int modbus_report_slave_id_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_READ_GENERAL_REFERENCE */

typedef struct modbus_read_general_reference_cmd {
  uint8_t link_type;
  uint16_t filenum;
  uint16_t address;
  uint16_t count;
} modbus_read_general_reference_cmd_t;

static int modbus_parse_read_general_reference_command(const uint8_t *data, uint8_t dlen, struct modbus_read_general_reference_cmd *command);

static int modbus_read_general_reference(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_WRITE_GENERAL_REFERENCE */

typedef struct modbus_write_general_reference_cmd {
  uint8_t link_type;
  uint16_t filenum;
  uint16_t address;
  uint16_t count;
} modbus_write_general_reference_cmd_t;

static int modbus_parse_write_general_reference_command(const uint8_t *data, uint8_t dlen, struct modbus_write_general_reference_cmd *command);

static int modbus_write_general_reference(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_MASK_WRITE_REGISTER */

typedef struct modbus_mask_write_command {
  uint16_t address;
  uint16_t and_mask;
  uint16_t or_mask;
} modbus_mask_write_command_t;

static int modbus_parse_mask_write_reg_command(const uint8_t *data, uint8_t dlen, struct modbus_mask_write_command *command);

static int modbus_mask_write_reg_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* MODBUS_WRITE_AND_READ_REGISTERS */

typedef struct modbus_write_read_regs_command {
  uint16_t read_address;
  uint16_t read_count;
  uint16_t write_address;
  uint16_t write_count;
  uint8_t bytes;
} modbus_write_read_regs_command_t;

static int modbus_parse_write_read_regs_command(const uint8_t *data, uint8_t dlen, struct modbus_write_read_regs_command *command);

static int modbus_write_read_regs_cmd(struct modbus_instance *instance, struct modbus_request *req);

/* CRC-16 */

/* Table of CRC values for high-order byte */
static const uint8_t table_crc_hi[] = {
  0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
  0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
  0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
  0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
  0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1,
  0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41,
  0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1,
  0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
  0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
  0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40,
  0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1,
  0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
  0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
  0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40,
  0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
  0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40,
  0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
  0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
  0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
  0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
  0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0,
  0x80, 0x41, 0x00, 0xC1, 0x81, 0x40, 0x00, 0xC1, 0x81, 0x40,
  0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0, 0x80, 0x41, 0x00, 0xC1,
  0x81, 0x40, 0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41,
  0x00, 0xC1, 0x81, 0x40, 0x01, 0xC0, 0x80, 0x41, 0x01, 0xC0,
  0x80, 0x41, 0x00, 0xC1, 0x81, 0x40
};

/* Table of CRC values for low-order byte */
static const uint8_t table_crc_lo[] = {
  0x00, 0xC0, 0xC1, 0x01, 0xC3, 0x03, 0x02, 0xC2, 0xC6, 0x06,
  0x07, 0xC7, 0x05, 0xC5, 0xC4, 0x04, 0xCC, 0x0C, 0x0D, 0xCD,
  0x0F, 0xCF, 0xCE, 0x0E, 0x0A, 0xCA, 0xCB, 0x0B, 0xC9, 0x09,
  0x08, 0xC8, 0xD8, 0x18, 0x19, 0xD9, 0x1B, 0xDB, 0xDA, 0x1A,
  0x1E, 0xDE, 0xDF, 0x1F, 0xDD, 0x1D, 0x1C, 0xDC, 0x14, 0xD4,
  0xD5, 0x15, 0xD7, 0x17, 0x16, 0xD6, 0xD2, 0x12, 0x13, 0xD3,
  0x11, 0xD1, 0xD0, 0x10, 0xF0, 0x30, 0x31, 0xF1, 0x33, 0xF3,
  0xF2, 0x32, 0x36, 0xF6, 0xF7, 0x37, 0xF5, 0x35, 0x34, 0xF4,
  0x3C, 0xFC, 0xFD, 0x3D, 0xFF, 0x3F, 0x3E, 0xFE, 0xFA, 0x3A,
  0x3B, 0xFB, 0x39, 0xF9, 0xF8, 0x38, 0x28, 0xE8, 0xE9, 0x29,
  0xEB, 0x2B, 0x2A, 0xEA, 0xEE, 0x2E, 0x2F, 0xEF, 0x2D, 0xED,
  0xEC, 0x2C, 0xE4, 0x24, 0x25, 0xE5, 0x27, 0xE7, 0xE6, 0x26,
  0x22, 0xE2, 0xE3, 0x23, 0xE1, 0x21, 0x20, 0xE0, 0xA0, 0x60,
  0x61, 0xA1, 0x63, 0xA3, 0xA2, 0x62, 0x66, 0xA6, 0xA7, 0x67,
  0xA5, 0x65, 0x64, 0xA4, 0x6C, 0xAC, 0xAD, 0x6D, 0xAF, 0x6F,
  0x6E, 0xAE, 0xAA, 0x6A, 0x6B, 0xAB, 0x69, 0xA9, 0xA8, 0x68,
  0x78, 0xB8, 0xB9, 0x79, 0xBB, 0x7B, 0x7A, 0xBA, 0xBE, 0x7E,
  0x7F, 0xBF, 0x7D, 0xBD, 0xBC, 0x7C, 0xB4, 0x74, 0x75, 0xB5,
  0x77, 0xB7, 0xB6, 0x76, 0x72, 0xB2, 0xB3, 0x73, 0xB1, 0x71,
  0x70, 0xB0, 0x50, 0x90, 0x91, 0x51, 0x93, 0x53, 0x52, 0x92,
  0x96, 0x56, 0x57, 0x97, 0x55, 0x95, 0x94, 0x54, 0x9C, 0x5C,
  0x5D, 0x9D, 0x5F, 0x9F, 0x9E, 0x5E, 0x5A, 0x9A, 0x9B, 0x5B,
  0x99, 0x59, 0x58, 0x98, 0x88, 0x48, 0x49, 0x89, 0x4B, 0x8B,
  0x8A, 0x4A, 0x4E, 0x8E, 0x8F, 0x4F, 0x8D, 0x4D, 0x4C, 0x8C,
  0x44, 0x84, 0x85, 0x45, 0x87, 0x47, 0x46, 0x86, 0x82, 0x42,
  0x43, 0x83, 0x41, 0x81, 0x80, 0x40
};

/* IMPLEMENTATION */

static uint16_t crc16(const uint8_t *buffer, uint16_t buffer_length)
{
  uint8_t crc_hi = 0xFF; /* high CRC byte initialized */
  uint8_t crc_lo = 0xFF; /* low CRC byte initialized */
  unsigned int i; /* will index into CRC lookup */
  
  /* pass through message buffer */
  while (buffer_length--)
  {
    i = crc_hi ^ *buffer++; /* calculate the CRC  */
    crc_hi = crc_lo ^ table_crc_hi[i];
    crc_lo = table_crc_lo[i];
  }
  
  return (crc_hi << 8 | crc_lo);
}

// TRANSPORT PACKET MINIMUM LENGTH
#define MODBUS_RTU_PACKET_MIN_LEN 0x04
#define MODBUS_TCP_PACKET_MIN_LEN 0x09

// RTU REQUEST
#define MODBUS_RTU_ADDRESS_OFFSET 0x00
#define MODBUS_RTU_FUNCTION_OFFSET 0x01
#define MODBUS_RTU_DATA_OFFSET 0x02

// TCP REQUEST
#define MODBUS_TCP_TRANSACTION_ID_OFFSET 0x00
#define MODBUS_TCP_PROTOCOL_OFFSET 0x02
#define MODBUS_TCP_LENGTH_OFFSET 0x04
#define MODBUS_TCP_UNIT_IDENT_OFFSET 0x06
#define MODBUS_TCP_FUNCTION_OFFSET 0x07
#define MODBUS_TCP_DATA_OFFSET 0x08

#define MODBUS_TCP_PROTOCOL_ID 0x0000

static size_t modbus_start_answer(struct modbus_instance *instance, uint8_t function)
{
  switch (instance->transport)
  {
  case MODBUS_RTU:
    instance->send_buffer[MODBUS_RTU_ADDRESS_OFFSET] = instance->address;
    instance->send_buffer[MODBUS_RTU_FUNCTION_OFFSET] = function;
    return 2;
  case MODBUS_TCP:
    memcpy(instance->send_buffer, instance->recv_buffer, MODBUS_TCP_FUNCTION_OFFSET);
    instance->send_buffer[MODBUS_TCP_FUNCTION_OFFSET] = function;
    return (MODBUS_TCP_FUNCTION_OFFSET+1);
  default:
    return 0;
  }
}

static size_t modbus_end_answer(struct modbus_instance *instance, size_t len)
{
  uint16_t crc_calculated;
  
  switch (instance->transport)
  {
  case MODBUS_RTU:
    crc_calculated = crc16(instance->send_buffer, len);
    
    instance->send_buffer[len++] = (crc_calculated >> 8);
    instance->send_buffer[len++] = crc_calculated;
    
    break;
  case MODBUS_TCP:
    (void)crc_calculated;
    
    instance->send_buffer[MODBUS_TCP_LENGTH_OFFSET] = (len-MODBUS_TCP_FUNCTION_OFFSET-1) >> 8;
    instance->send_buffer[MODBUS_TCP_LENGTH_OFFSET + 1] = (len-MODBUS_TCP_FUNCTION_OFFSET-1) & 0xFF;
    
    break;
  }
  
  return len;
}

static int modbus_send_answer(struct modbus_instance *instance, size_t len, uint8_t broadcast)
{
  const struct modbus_functions *functions = instance->functions;

  if (broadcast)
  {
    MODBUS_RETURN(instance, MODBUS_SUCCESS);
  }
  else
  {
    return functions->write(instance, instance->send_buffer, len);
  }
}

static int modbus_send_error(struct modbus_instance *instance, uint8_t function, uint8_t error, uint8_t broadcast)
{
  size_t len = modbus_start_answer(instance, 0x80 | function);
  
  instance->send_buffer[len++] = error;
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, broadcast);
}

#define MODBUS_READ_CMD_ADDRESS_OFFSET 0x00
#define MODBUS_READ_CMD_COUNT_OFFSET 0x02

static int modbus_parse_read_command(const uint8_t *data, uint8_t dlen,
                                     struct modbus_read_command * command)
{
  if (command == NULL)
    return -1;
  
  if (dlen != 4)
    return -1;
  
  command->address = data[MODBUS_READ_CMD_ADDRESS_OFFSET + 1] | (data[MODBUS_READ_CMD_ADDRESS_OFFSET] << 8);
  command->count = data[MODBUS_READ_CMD_COUNT_OFFSET + 1] | (data[MODBUS_READ_CMD_COUNT_OFFSET] << 8);
  
  return 0;
}

static int _modbus_read_io_status(uint16_t address, int count,
                                  uint8_t *tab,
                                  uint8_t *send_buffer, int offset)
{
  int shift = 0;
  int one_byte = 0;
  int i;
  
  for (i = address; i < address + count; i++)
  {
    one_byte |= tab[i] << shift;
    if (shift == 7)
    {
      send_buffer[offset++] = one_byte;
      one_byte = shift = 0;
    } else {
      shift++;
    }
  }
  
  if (shift != 0)
    send_buffer[offset++] = one_byte;
  
  return offset;
}

static int _modbus_find_bits(const struct modbus_bits_subtable *table,
                             uint16_t address, uint16_t count, uint8_t **bits)
{
  uint16_t i = 0;
  uint16_t last_address = address + count;
  
  if (!table)
    return -1;
  
  while (table[i].address != 0
         || table[i].count != 0
           || table[i].bits != NULL)
  {
    if ((address >= table[i].address && address < (table[i].address + table[i].count))
        && (last_address > table[i].address && last_address <= (table[i].address + table[i].count)))
    {
      *bits = table[i].bits;
      return 0;
    }
    ++i;
  }
  
  return -1;
}

static int _modbus_read_bits(struct modbus_instance *instance, struct modbus_request *req,
                             enum modbus_table table)
{
  struct modbus_read_command command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  size_t i;
  uint8_t *bits;
  
  int ret = modbus_parse_read_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (command.count < 1)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_VALUE, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  for (i = 0; i < command.count; ++i)
  {
    if (_modbus_find_bits((table == MODBUS_TABLE_COILS? instance->coil_table: instance->discrete_table),
                          command.address + i, 1U, &bits) < 0)
    {
      modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
      
      MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
    }
  }
  
  if (functions->before_read_table)
  {
    ret = functions->before_read_table(instance, table, command.address, command.count);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  instance->send_buffer[len++] = (command.count >> 3) + ((command.count & 7)? 1: 0);
  
  if (functions->lock)
    functions->lock(instance, table, MODBUS_LOCK);
  
  for (i = 0; i < command.count; ++i)
  {
    (void)_modbus_find_bits((table == MODBUS_TABLE_COILS? instance->coil_table: instance->discrete_table),
                            command.address + i, 1U, &bits);
     len = _modbus_read_io_status(command.address + i, 1U, bits, instance->send_buffer, len);
  }
  
  if (functions->lock)
    functions->lock(instance, table, MODBUS_UNLOCK);
  
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

static int modbus_read_coils_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  return _modbus_read_bits(instance, req, MODBUS_TABLE_COILS);
}

static int modbus_read_discrete_inputs_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  return _modbus_read_bits(instance, req, MODBUS_TABLE_DISCRETE_INPUTS);
}

static int _modbus_find_regs(const struct modbus_regs_subtable *table,
                             uint16_t address, uint16_t count, uint16_t **regs, uint16_t *offset)
{
  uint16_t i = 0;
  uint16_t last_address = address + count;
  
  if (!table)
    return -1;
  
  while (table[i].address != 0
         || table[i].count != 0
           || table[i].regs != NULL)
  {
    if ((address >= table[i].address && address < (table[i].address + table[i].count))
        && (last_address > table[i].address && last_address <= (table[i].address + table[i].count)))
    {
      *regs = table[i].regs;
      *offset = address - table[i].address;
      return 0;
    }
    ++i;
  }
  
  return -1;
}

static int _modbus_read_regs(struct modbus_instance *instance, struct modbus_request *req,
                             enum modbus_table table)
{
  struct modbus_read_command command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  size_t i;
  uint16_t *regs;
  uint16_t offset;
  
  int ret = modbus_parse_read_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (command.count < 1)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_VALUE, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  for (i = 0; i < command.count; ++i)
  {
    if (_modbus_find_regs((table == MODBUS_TABLE_INPUT_REGISTERS? instance->input_table: instance->holding_table),
                          command.address + i, 1U, &regs, &offset) < 0)
    {
      modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
      
      MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
    }
  }
  
  if (functions->before_read_table)
  {
    ret = functions->before_read_table(instance, table, command.address, command.count);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  instance->send_buffer[len++] = command.count * 2;
  
  if (functions->lock)
    functions->lock(instance, table, MODBUS_LOCK);
  
  for (i = 0; i < command.count; ++i)
  {
    (void)_modbus_find_regs((table == MODBUS_TABLE_INPUT_REGISTERS? instance->input_table: instance->holding_table),
                            command.address + i, 1U, &regs, &offset);
     
     instance->send_buffer[len++] = regs[offset] >> 8;
     instance->send_buffer[len++] = regs[offset] & 0xFF;
  }
  
  if (functions->lock)
    functions->lock(instance, table, MODBUS_UNLOCK);
  
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

static int modbus_read_holding_registers_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  return _modbus_read_regs(instance, req, MODBUS_TABLE_HOLDING_REGISTERS);
}

static int modbus_read_input_registers_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  return _modbus_read_regs(instance, req, MODBUS_TABLE_INPUT_REGISTERS);
}

#define MODBUS_WRITE_BIT_CMD_ADDRESS_OFFSET 0x00
#define MODBUS_WRITE_BIT_CMD_DATA_OFFSET 0x02

static int modbus_parse_write_bit_command(const uint8_t *data, uint8_t dlen,
                                          struct modbus_write_bit_command *command)
{
  if (command == NULL)
    return -1;
  
  if (dlen != 4)
    return -1;
  
  command->address = data[MODBUS_WRITE_BIT_CMD_ADDRESS_OFFSET + 1] | (data[MODBUS_WRITE_BIT_CMD_ADDRESS_OFFSET] << 8);
  command->data = data[MODBUS_WRITE_BIT_CMD_DATA_OFFSET + 1] | (data[MODBUS_WRITE_BIT_CMD_DATA_OFFSET] << 8);
  
  return 0;
}

static int modbus_write_coil_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  struct modbus_write_bit_command command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  uint8_t *bits;
  
  int ret = modbus_parse_write_bit_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (command.data != 0xFF00 && command.data != 0x0000)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_VALUE, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  if (_modbus_find_bits(instance->coil_table, command.address, 1U, &bits) < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_COILS, MODBUS_LOCK);
  
  bits[command.address] = command.data? 1U: 0U;
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_COILS, MODBUS_UNLOCK);
  
  if (functions->after_write_table)
  {
    ret = functions->after_write_table(instance, MODBUS_TABLE_COILS, command.address, 1U);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  memcpy(instance->send_buffer + len, req->data, req->dlen);
  len += req->dlen;
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

#define MODBUS_WRITE_REG_CMD_ADDRESS_OFFSET 0x00
#define MODBUS_WRITE_REG_CMD_DATA_OFFSET 0x02

static int modbus_parse_write_reg_command(const uint8_t *data, uint8_t dlen,
                                          struct modbus_write_reg_command *command)
{
  if (command == NULL)
    return -1;
  
  if (dlen != 4)
    return -1;
  
  command->address = data[MODBUS_WRITE_REG_CMD_ADDRESS_OFFSET + 1] | (data[MODBUS_WRITE_REG_CMD_ADDRESS_OFFSET] << 8);
  command->data = data[MODBUS_WRITE_REG_CMD_DATA_OFFSET + 1] | (data[MODBUS_WRITE_REG_CMD_DATA_OFFSET] << 8);
  
  return 0;
}

static int modbus_write_register_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  struct modbus_write_reg_command command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  uint16_t *regs;
  uint16_t offset;
  
  int ret = modbus_parse_write_reg_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (_modbus_find_regs(instance->holding_table, command.address, 1U, &regs, &offset) < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_HOLDING_REGISTERS, MODBUS_LOCK);
  
  regs[offset] = command.data;
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_HOLDING_REGISTERS, MODBUS_UNLOCK);
  
  if (functions->after_write_table)
  {
    ret = functions->after_write_table(instance, MODBUS_TABLE_HOLDING_REGISTERS, command.address, 1U);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  memcpy(instance->send_buffer + len, req->data, req->dlen);
  len += req->dlen;
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

#define MODBUS_WRITE_MULTIPLE_CMD_ADDRESS_OFFSET 0x00
#define MODBUS_WRITE_MULTIPLE_CMD_COUNT_OFFSET 0x02
#define MODBUS_WRITE_MULTIPLE_CMD_BYTES_OFFSET 0x04

static int modbus_parse_write_multiple_command(const uint8_t *data, uint8_t dlen,
                                               struct modbus_write_multiple_command *command)
{
  if (command == NULL)
    return -1;
  
  if (dlen < 5)
    return -1;
  
  command->address = data[MODBUS_WRITE_MULTIPLE_CMD_ADDRESS_OFFSET + 1] | (data[MODBUS_WRITE_MULTIPLE_CMD_ADDRESS_OFFSET] << 8);
  command->count = data[MODBUS_WRITE_MULTIPLE_CMD_COUNT_OFFSET + 1] | (data[MODBUS_WRITE_MULTIPLE_CMD_COUNT_OFFSET] << 8);
  command->bytes = data[MODBUS_WRITE_MULTIPLE_CMD_BYTES_OFFSET];
  
  return 0;
}

static void _modbus_set_bits_from_bytes(uint8_t *tbits, uint16_t address, uint16_t count,
                                        const uint8_t *data)
{
  unsigned int i;
  int shift = 0;
  
  for (i = address; i < address + count; i++) {
    tbits[i] = data[(i - address) / 8] & (1 << shift) ? 1 : 0;
    /* gcc doesn't like: shift = (++shift) % 8; */
    shift++;
    shift %= 8;
  }
}

#define MODBUS_WRITE_MULTIPLE_DATA_OFFSET 0x05

static int modbus_write_multiple_coils_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  struct modbus_write_multiple_command command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  size_t i;
  uint8_t *bits;
  
  int ret = modbus_parse_write_multiple_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (command.count < 1)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_VALUE, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  for (i = 0; i < command.count; ++i)
  {
    if (_modbus_find_bits(instance->coil_table, command.address + i, 1U, &bits) < 0)
    {
      modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
      
      MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
    }
  }
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_COILS, MODBUS_LOCK);
  
  for (i = 0; i < command.count; ++i)
  {
    (void)_modbus_find_bits(instance->coil_table, command.address + i, 1U, &bits);
    _modbus_set_bits_from_bytes(bits, command.address + i, 1U, req->data + MODBUS_WRITE_MULTIPLE_DATA_OFFSET);
  }
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_COILS, MODBUS_UNLOCK);
  
  if (functions->after_write_table)
  {
    ret = functions->after_write_table(instance, MODBUS_TABLE_COILS, command.address, command.count);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  memcpy(instance->send_buffer + len, req->data, MODBUS_WRITE_MULTIPLE_DATA_OFFSET - 1);
  len += MODBUS_WRITE_MULTIPLE_DATA_OFFSET - 1;
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

static int modbus_write_multiple_regs_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  struct modbus_write_multiple_command command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  size_t i;
  size_t j;
  uint16_t *regs;
  uint16_t offset;
  
  int ret = modbus_parse_write_multiple_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (command.count < 1)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_VALUE, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  for (i = 0; i < command.count; ++i)
  {
    if (_modbus_find_regs(instance->holding_table, command.address + i, 1U, &regs, &offset) < 0)
    {
      modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
      
      MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
    }
  }
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_HOLDING_REGISTERS, MODBUS_LOCK);
  
  for (i = 0, j = 0; i < command.count; ++i, j += 2)
  {
    (void)_modbus_find_regs(instance->holding_table, command.address + i, 1U, &regs, &offset);
    /* 6 and 7 = first value */
    regs[offset] = (req->data[MODBUS_WRITE_MULTIPLE_DATA_OFFSET + j] << 8)
      + req->data[MODBUS_WRITE_MULTIPLE_DATA_OFFSET + j + 1];
  }
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_HOLDING_REGISTERS, MODBUS_UNLOCK);
  
  if (functions->after_write_table)
  {
    ret = functions->after_write_table(instance, MODBUS_TABLE_HOLDING_REGISTERS, command.address, command.count);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  memcpy(instance->send_buffer + len, req->data, MODBUS_WRITE_MULTIPLE_DATA_OFFSET - 1);
  len += MODBUS_WRITE_MULTIPLE_DATA_OFFSET - 1;
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

#define MODBUS_SLAVE_ID 180

static int modbus_report_slave_id_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  size_t len = modbus_start_answer(instance, req->function);
  size_t bytes_pos = len++;
  instance->send_buffer[len++] = MODBUS_SLAVE_ID;
  instance->send_buffer[len++] = 0xFF;
  if (instance->id)
  {
    size_t slen = strlen(instance->id);
    memcpy(instance->send_buffer + len, instance->id, slen);
    len += slen;
  }
  
  instance->send_buffer[bytes_pos] = len + 2;
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

#define MODBUS_READ_GENERAL_REFERENCE_CMD_LINK_TYPE_OFFSET 0x00
#define MODBUS_READ_GENERAL_REFERENCE_CMD_FILENUM_OFFSET 0x01
#define MODBUS_READ_GENERAL_REFERENCE_CMD_ADDRESS_OFFSET 0x03
#define MODBUS_READ_GENERAL_REFERENCE_CMD_COUNT_OFFSET 0x05

static int modbus_parse_read_general_reference_command(const uint8_t *data, uint8_t dlen, struct modbus_read_general_reference_cmd *command)
{
  if (command == NULL)
    return -1;
  
  if (dlen < 8)
    return -1;
  
  ++data;
  
  command->link_type = data[MODBUS_READ_GENERAL_REFERENCE_CMD_LINK_TYPE_OFFSET];
  command->filenum = data[MODBUS_READ_GENERAL_REFERENCE_CMD_FILENUM_OFFSET + 1] | (data[MODBUS_READ_GENERAL_REFERENCE_CMD_FILENUM_OFFSET] << 8);
  command->address = data[MODBUS_READ_GENERAL_REFERENCE_CMD_ADDRESS_OFFSET + 1] | (data[MODBUS_READ_GENERAL_REFERENCE_CMD_ADDRESS_OFFSET] << 8);
  command->count = data[MODBUS_READ_GENERAL_REFERENCE_CMD_COUNT_OFFSET + 1] | (data[MODBUS_READ_GENERAL_REFERENCE_CMD_COUNT_OFFSET] << 8);
  
  return 0;
}

static int modbus_read_general_reference(struct modbus_instance *instance, struct modbus_request *req)
{
  struct modbus_read_general_reference_cmd command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  int ret;
  
  if (!functions->read_file)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_NOT_IMPLEMENTED);
  }
  
  ret = modbus_parse_read_general_reference_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (command.link_type != 0x06
      || command.count < 1)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_VALUE, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  len = modbus_start_answer(instance, req->function);
  instance->send_buffer[len] = (command.count << 1) + 2;
  ++len;
  instance->send_buffer[len] = (command.count << 1) + 1;
  ++len;
  instance->send_buffer[len] = 0x06;
  ++len;
  
  ret = functions->read_file(instance, command.filenum, command.address, command.count, instance->send_buffer + len);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len += (command.count << 1);
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

#define MODBUS_WRITE_GENERAL_REFERENCE_CMD_LINK_TYPE_OFFSET 0x01
#define MODBUS_WRITE_GENERAL_REFERENCE_CMD_FILENUM_OFFSET 0x02
#define MODBUS_WRITE_GENERAL_REFERENCE_CMD_ADDRESS_OFFSET 0x04
#define MODBUS_WRITE_GENERAL_REFERENCE_CMD_COUNT_OFFSET 0x06
#define MODBUS_WRITE_GENERAL_REFERENCE_CMD_DATA_OFFSET 0x08

static int modbus_parse_write_general_reference_command(const uint8_t *data, uint8_t dlen, struct modbus_write_general_reference_cmd *command)
{
  if (command == NULL)
    return -1;
  
  if (dlen <= 8)
    return -1;
  
  command->link_type = data[MODBUS_WRITE_GENERAL_REFERENCE_CMD_LINK_TYPE_OFFSET];
  command->filenum = data[MODBUS_WRITE_GENERAL_REFERENCE_CMD_FILENUM_OFFSET + 1] | (data[MODBUS_WRITE_GENERAL_REFERENCE_CMD_FILENUM_OFFSET] << 8);
  command->address = data[MODBUS_WRITE_GENERAL_REFERENCE_CMD_ADDRESS_OFFSET + 1] | (data[MODBUS_WRITE_GENERAL_REFERENCE_CMD_ADDRESS_OFFSET] << 8);
  command->count = data[MODBUS_WRITE_GENERAL_REFERENCE_CMD_COUNT_OFFSET + 1] | (data[MODBUS_WRITE_GENERAL_REFERENCE_CMD_COUNT_OFFSET] << 8);
  
  return 0;
}

static int modbus_write_general_reference(struct modbus_instance *instance, struct modbus_request *req)
{
  struct modbus_write_general_reference_cmd command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  int ret;
  
  if (!functions->read_file)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_NOT_IMPLEMENTED);
  }
  
  ret = modbus_parse_write_general_reference_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (command.link_type != 0x06
      || command.count < 1)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_VALUE, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  ret = functions->write_file(instance, command.filenum, command.address, command.count, req->data + MODBUS_WRITE_GENERAL_REFERENCE_CMD_DATA_OFFSET);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  memcpy(instance->send_buffer + len, req->data, req->dlen);
  len += req->dlen;
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

#define MODBUS_MASK_WRITE_CMD_ADDRESS_OFFSET 0x00
#define MODBUS_MASK_WRITE_CMD_AND_OFFSET 0x02
#define MODBUS_MASK_WRITE_CMD_OR_OFFSET 0x04

static int modbus_parse_mask_write_reg_command(const uint8_t *data, uint8_t dlen, struct modbus_mask_write_command *command)
{
  if (command == NULL)
    return -1;
  
  if (dlen != 6)
    return -1;
  
  command->address = data[MODBUS_MASK_WRITE_CMD_ADDRESS_OFFSET + 1] | (data[MODBUS_MASK_WRITE_CMD_ADDRESS_OFFSET] << 8);
  command->and_mask = data[MODBUS_MASK_WRITE_CMD_AND_OFFSET + 1] | (data[MODBUS_MASK_WRITE_CMD_AND_OFFSET] << 8);
  command->or_mask = data[MODBUS_MASK_WRITE_CMD_OR_OFFSET + 1] | (data[MODBUS_MASK_WRITE_CMD_OR_OFFSET] << 8);
  
  return 0;
}

static int modbus_mask_write_reg_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  struct modbus_mask_write_command command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  uint16_t *regs;
  uint16_t offset;
  uint16_t data;
  
  int ret = modbus_parse_mask_write_reg_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (_modbus_find_regs(instance->holding_table, command.address, 1U, &regs, &offset) < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_HOLDING_REGISTERS, MODBUS_LOCK);
  
  data = regs[offset];
  data = (data & command.and_mask) | (command.or_mask & (~command.and_mask));
  regs[offset] = data;
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_HOLDING_REGISTERS, MODBUS_UNLOCK);
  
  if (functions->after_write_table)
  {
    ret = functions->after_write_table(instance, MODBUS_TABLE_HOLDING_REGISTERS, command.address, 1U);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  memcpy(instance->send_buffer + len, req->data, req->dlen);
  len += req->dlen;
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

#define MODBUS_READ_WRITE_CMD_READ_ADDRESS_OFFSET 0x00
#define MODBUS_READ_WRITE_CMD_READ_COUNT_OFFSET 0x02
#define MODBUS_READ_WRITE_CMD_WRITE_ADDRESS_OFFSET 0x04
#define MODBUS_READ_WRITE_CMD_WRITE_COUNT_OFFSET 0x06
#define MODBUS_READ_WRITE_CMD_BYTES_OFFSET 0x08

static int modbus_parse_write_read_regs_command(const uint8_t *data, uint8_t dlen, struct modbus_write_read_regs_command *command)
{
  if (command == NULL)
    return -1;
  
  if (dlen < 9)
    return -1;
  
  command->read_address = data[MODBUS_READ_WRITE_CMD_READ_ADDRESS_OFFSET + 1] | (data[MODBUS_READ_WRITE_CMD_READ_ADDRESS_OFFSET] << 8);
  command->read_count = data[MODBUS_READ_WRITE_CMD_READ_COUNT_OFFSET + 1] | (data[MODBUS_READ_WRITE_CMD_READ_COUNT_OFFSET] << 8);
  command->write_address = data[MODBUS_READ_WRITE_CMD_WRITE_ADDRESS_OFFSET + 1] | (data[MODBUS_READ_WRITE_CMD_WRITE_ADDRESS_OFFSET] << 8);
  command->write_count = data[MODBUS_READ_WRITE_CMD_WRITE_COUNT_OFFSET + 1] | (data[MODBUS_READ_WRITE_CMD_WRITE_COUNT_OFFSET] << 8);
  command->bytes = data[MODBUS_READ_WRITE_CMD_BYTES_OFFSET];
  
  return 0;
}

static int modbus_write_read_regs_cmd(struct modbus_instance *instance, struct modbus_request *req)
{
  struct modbus_write_read_regs_command command;
  const struct modbus_functions *functions = instance->functions;
  size_t len;
  size_t i;
  size_t j;
  uint16_t *write_regs;
  uint16_t write_offset;
  uint16_t *read_regs;
  uint16_t read_offset;
  
  int ret = modbus_parse_write_read_regs_command(req->data, req->dlen, &command);
  if (ret < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_COMMAND);
  }
  
  if (command.read_count < 1
      || command.write_count < 1
        || command.bytes != command.write_count * 2)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_VALUE, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  if (_modbus_find_regs(instance->holding_table, command.read_address, command.read_count, &read_regs, &read_offset) < 0
      || _modbus_find_regs(instance->holding_table, command.write_address, command.write_count, &write_regs, &write_offset) < 0)
  {
    modbus_send_error(instance, req->function, MODBUS_ERROR_ILLEGAL_DATA_ADDRESS, IS_BROADCAST(req));
    
    MODBUS_RETURN(instance, MODBUS_BAD_PARAMS);
  }
  
  if (functions->before_read_table)
  {
    ret = functions->before_read_table(instance, MODBUS_TABLE_HOLDING_REGISTERS, command.read_address, command.read_count);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_start_answer(instance, req->function);
  instance->send_buffer[len++] = command.read_count * 2;
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_HOLDING_REGISTERS, MODBUS_LOCK);
  
  for (i = command.write_address, j = 0; i < command.write_address + command.write_count; i++, j += 2, write_offset++) {
    write_regs[write_offset] = (req->data[MODBUS_WRITE_MULTIPLE_DATA_OFFSET + j] << 8) +
      req->data[MODBUS_WRITE_MULTIPLE_DATA_OFFSET + j + 1];
  }
  
  for (i = command.read_address; i < command.read_address + command.read_count; i++, read_offset++) {
    instance->send_buffer[len++] = read_regs[read_offset] >> 8;
    instance->send_buffer[len++] = read_regs[read_offset] & 0xFF;
  }
  
  if (functions->lock)
    functions->lock(instance, MODBUS_TABLE_HOLDING_REGISTERS, MODBUS_UNLOCK);
  
  if (functions->after_write_table)
  {
    ret = functions->after_write_table(instance, MODBUS_TABLE_HOLDING_REGISTERS, command.write_address, command.write_count);
    if (ret < 0)
      MODBUS_RETURN(instance, MODBUS_INT);
  }
  
  len = modbus_end_answer(instance, len);
  
  return modbus_send_answer(instance, len, IS_BROADCAST(req));
}

/* IO */

int modbus_io(struct modbus_instance *instance)
{
  uint16_t crc_calculated;
  uint16_t crc_received;
  uint8_t *packet;
  const struct modbus_functions *functions;
  int plen;
  struct modbus_request req;
  int ret;
  
  if (!instance)
    return -1;
  
  functions = instance->functions;
  if (!functions)
    MODBUS_RETURN(instance, MODBUS_INVAL);
  
  if (!instance->functions->read)
    MODBUS_RETURN(instance, MODBUS_INVAL);
  
  packet = instance->recv_buffer;
  plen = functions->read(instance, packet, instance->recv_buffer_size);
  
  switch (instance->transport)
  {
  case MODBUS_RTU:
    if (plen < MODBUS_RTU_PACKET_MIN_LEN)
      MODBUS_RETURN(instance, MODBUS_NO_PACKET);
    
    crc_calculated = crc16(packet, plen - 2);
    crc_received = (packet[plen - 2] << 8) | packet[plen - 1];
    
    if (crc_calculated != crc_received)
      MODBUS_RETURN(instance, MODBUS_BAD_CRC);
    
    if (packet[MODBUS_RTU_ADDRESS_OFFSET] != instance->address)
      MODBUS_RETURN(instance, MODBUS_BAD_ADDRESS);
    
    req.address = packet[MODBUS_RTU_ADDRESS_OFFSET];
    req.function = (enum modbus_request_function)packet[MODBUS_RTU_FUNCTION_OFFSET];
    req.data = &packet[MODBUS_RTU_DATA_OFFSET];
    req.dlen = plen - MODBUS_RTU_DATA_OFFSET - 2;
    break;
  case MODBUS_TCP:
    if (plen < MODBUS_TCP_PACKET_MIN_LEN)
      MODBUS_RETURN(instance, MODBUS_NO_PACKET);
    
    if (((uint16_t)packet[MODBUS_TCP_PROTOCOL_OFFSET] << 8) | ((uint16_t)packet[MODBUS_TCP_PROTOCOL_OFFSET + 1]) != MODBUS_TCP_PROTOCOL_ID)
      MODBUS_RETURN(instance, MODBUS_NO_PACKET);
    
    req.address = instance->address; /* address is not used */
    req.function = (enum modbus_request_function)packet[MODBUS_TCP_FUNCTION_OFFSET];
    req.data = &packet[MODBUS_TCP_DATA_OFFSET];
    req.dlen = plen - MODBUS_TCP_FUNCTION_OFFSET - 1;
    break;
  default:
    MODBUS_RETURN(instance, MODBUS_INVAL);
  }
  
  if (functions->open)
  {
    if ((ret = functions->open(instance)) < 0)
      return ret;
  }
  
  switch (req.function)
  {
  case MODBUS_READ_COILS:
    ret = modbus_read_coils_cmd(instance, &req);
    break;
  case MODBUS_READ_DISCRETE_INPUTS:
    ret = modbus_read_discrete_inputs_cmd(instance, &req);
    break;
  case MODBUS_READ_HOLDING_REGISTERS:
    ret = modbus_read_holding_registers_cmd(instance, &req);
    break;
  case MODBUS_READ_INPUT_REGISTERS:
    ret = modbus_read_input_registers_cmd(instance, &req);
    break;
    
  case MODBUS_WRITE_SINGLE_COIL:
    ret = modbus_write_coil_cmd(instance, &req);
    break;
  case MODBUS_WRITE_SINGLE_REGISTER:
    ret = modbus_write_register_cmd(instance, &req);
    break;
    
  case MODBUS_READ_EXCEPTION_STATUS:
    instance->error = MODBUS_NOT_IMPLEMENTED;
    ret = -1;
    break;
    
  case MODBUS_WRITE_MULTIPLE_COILS:
    ret = modbus_write_multiple_coils_cmd(instance, &req);
    break;
  case MODBUS_WRITE_MULTIPLE_REGISTERS:
    ret = modbus_write_multiple_regs_cmd(instance, &req);
    break;
    
  case MODBUS_REPORT_SLAVE_ID:
    ret = modbus_report_slave_id_cmd(instance, &req);
    break;
    
  case MODBUS_READ_GENERAL_REFERENCE:
    ret = modbus_read_general_reference(instance, &req);
    break;
    
  case MODBUS_WRITE_GENERAL_REFERENCE:
    ret = modbus_write_general_reference(instance, &req);
    break;
    
  case MODBUS_MASK_WRITE_REGISTER:
    ret = modbus_mask_write_reg_cmd(instance, &req);
    break;
    
  case MODBUS_WRITE_AND_READ_REGISTERS:
    ret = modbus_write_read_regs_cmd(instance, &req);
    break;
    
  default:
    ret = modbus_send_error(instance, req.function, MODBUS_ERROR_ILLEGAL_FUNCTION, IS_BROADCAST(&req));
    break;
  }
  
  if (functions->close)
    (void)functions->close(instance);
  
  return ret;
}
