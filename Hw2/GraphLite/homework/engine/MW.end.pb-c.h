/* Generated by the protocol buffer compiler.  DO NOT EDIT! */

#ifndef PROTOBUF_C_MW_2eend_2eproto__INCLUDED
#define PROTOBUF_C_MW_2eend_2eproto__INCLUDED

#include <google/protobuf-c/protobuf-c.h>

PROTOBUF_C_BEGIN_DECLS


typedef struct _Mw__End Mw__End;


/* --- enums --- */


/* --- messages --- */

struct  _Mw__End
{
  ProtobufCMessage base;
  int32_t s_id;
  int32_t d_id;
  int32_t state;
};
#define MW__END__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&mw__end__descriptor) \
    , 0, 0, 0 }


/* Mw__End methods */
void   mw__end__init
                     (Mw__End         *message);
size_t mw__end__get_packed_size
                     (const Mw__End   *message);
size_t mw__end__pack
                     (const Mw__End   *message,
                      uint8_t             *out);
size_t mw__end__pack_to_buffer
                     (const Mw__End   *message,
                      ProtobufCBuffer     *buffer);
Mw__End *
       mw__end__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   mw__end__free_unpacked
                     (Mw__End *message,
                      ProtobufCAllocator *allocator);
/* --- per-message closures --- */

typedef void (*Mw__End_Closure)
                 (const Mw__End *message,
                  void *closure_data);

/* --- services --- */


/* --- descriptors --- */

extern const ProtobufCMessageDescriptor mw__end__descriptor;

PROTOBUF_C_END_DECLS


#endif  /* PROTOBUF_MW_2eend_2eproto__INCLUDED */
