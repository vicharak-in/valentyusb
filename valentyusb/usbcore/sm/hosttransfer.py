#!/usr/bin/env python3

from migen import *
from migen.genlib import fsm
from migen.genlib.cdc import MultiReg

from ..endpoint import EndpointType, EndpointResponse
from ..pid import PID, PIDTypes
from ..rx.pipeline import RxPipeline
from ..tx.pipeline import TxPipeline
from ..sm.header import PacketHeaderDecode
from ..sm.send import TxPacketSend

class UsbHostTransfer(Module):

    def __init__(self, iobuf, auto_crc=True, cdc=False, low_speed_support=False):
        self.submodules.iobuf = iobuf = ClockDomainsRenamer("usb_48")(iobuf)
        if iobuf.usb_pullup is not None:
            self.comb += iobuf.usb_pullup.eq(0)
        self.submodules.tx = tx = TxPipeline(low_speed_support=low_speed_support)
        self.submodules.txstate = txstate = TxPacketSend(tx, auto_crc=auto_crc, token_support=True)
        self.submodules.rx = rx = RxPipeline(low_speed_support=low_speed_support)
        self.submodules.rxstate = rxstate = PacketHeaderDecode(rx)
        self.comb += [
            tx.i_bit_strobe.eq(rx.o_bit_strobe),
        ]

        self.data_recv_put = Signal()
        self.data_recv_payload = Signal(8)

        self.data_send_get = Signal()
        self.data_send_have = Signal()
        self.data_send_payload = Signal(8)
        self.data_end = Signal()

        self.i_addr = Signal(7)
        self.i_ep = Signal(4)
        self.i_frame = Signal(11)
        self.comb += [
            txstate.i_addr.eq(self.i_addr),
            txstate.i_ep.eq(self.i_ep),
            txstate.i_frame.eq(self.i_frame)
        ]

        self.i_reset = Signal()
        self.i_sof = Signal()

        self.i_cmd_setup = Signal()
        self.i_cmd_in = Signal()
        self.i_cmd_out = Signal()
        self.i_cmd_pre = Signal()
        self.i_cmd_data1 = Signal()
        self.i_cmd_iso = Signal()
        self.o_cmd_latched = Signal()

        self.o_got_ack = Signal()
        self.o_got_nak = Signal()
        self.o_got_stall = Signal()
        self.o_got_data0 = Signal()
        self.o_got_data1 = Signal()
        self.o_timeout = Signal()

        cmd_data1 = Signal()
        cmd_iso = Signal()
        # R2: bus-turnaround watchdog for WAIT_REPLY.  Counted in usb_12 clocks;
        # one bit time is 8 clocks at low speed and 1 at full speed, so this is
        # 64 bit times either way -- comfortably more than the 16..18 the spec
        # requires, but far below the 1ms frame interval.
        # Widened from 10 bits: the same counter now also bounds RECV_DATA,
        # which needs room for a whole data packet rather than a turnaround.
        reply_timeout = Signal(14)
        reply_limit = Signal(14)
        data_limit = Signal(14)
        # R3: RECV_DATA strobes every decoded byte, including the two CRC16
        # bytes at the end of the packet.  Delay the payload by two bytes so
        # the CRC is still sitting in the skid when the packet ends.
        recv_b0 = Signal(8)
        recv_b1 = Signal(8)
        recv_cnt = Signal(2)
        recv_strobe = Signal()
        low_speed_override = Signal()
        sof_latch = Signal()

        reset_out = Signal()
        self.specials += MultiReg(self.i_reset, reset_out, odomain="usb_48")

        if low_speed_support:
            self.i_low_speed = Signal()
            low_speed = Signal()
            self.comb += [
                low_speed.eq(self.i_low_speed | low_speed_override),
                rx.i_low_speed.eq(low_speed),
                tx.i_low_speed.eq(low_speed),
            ]
        else:
            low_speed = 0

        self.comb += [
            rx.i_usbp.eq(iobuf.usb_p_rx),
            rx.i_usbn.eq(iobuf.usb_n_rx),
            iobuf.usb_ls_rx.eq(low_speed),
            iobuf.usb_tx_en.eq(tx.o_oe | reset_out),
            iobuf.usb_p_tx.eq(tx.o_usbp & ~reset_out),
            iobuf.usb_n_tx.eq(tx.o_usbn & ~reset_out),
        ]

        self.sync.usb_12 += If(self.i_sof, sof_latch.eq(1))

        self.comb += reply_limit.eq(Mux(low_speed, 64*8, 64))

        # Budget for receiving a whole data packet, in usb_12 ticks.  The
        # largest is 8 sync + 8 pid + 512 data + 16 crc = 544 bits, which at low
        # speed is 8 ticks per bit.  Rounded up generously -- this is a
        # last-resort escape, not a protocol deadline.
        self.comb += data_limit.eq(Mux(low_speed, 8192, 1024))

        fsm = ResetInserter()(FSM(reset_state='IDLE'))
        self.submodules.fsm = fsm = ClockDomainsRenamer('usb_12')(fsm)
        self.comb += fsm.reset.eq(self.i_reset | rx.o_reset)

        fsm.act('IDLE',
                NextValue(low_speed_override, 0),
                If (sof_latch,
                    If (low_speed,
                        NextState('KA' if low_speed_support else 'SOF')
                    ).Else (NextState('SOF'))
                ).Elif (self.i_cmd_setup | self.i_cmd_in | self.i_cmd_out,
                   If (self.i_cmd_pre,
                       NextState('PREAMBLE')
                   ).Else (NextState('START_TRANSFER'))))

        fsm.act('SOF',
                txstate.i_pkt_start.eq(1),
                txstate.i_pid.eq(PID.SOF),
                If (txstate.o_pkt_end,
                    NextValue(sof_latch, 0),
                    NextState('IDLE')))

        fsm.act('PREAMBLE',
                txstate.i_pkt_start.eq(1),
                txstate.i_pid.eq(PID.PRE),
                If (txstate.o_pkt_end,
                    NextValue(low_speed_override, 1),
                    NextState('START_TRANSFER_AFTER_PREAMBLE')))

        fsm.delayed_enter('START_TRANSFER_AFTER_PREAMBLE', 'START_TRANSFER', 4)

        fsm.act('START_TRANSFER',
                self.o_cmd_latched.eq(1),
                NextValue(cmd_data1, self.i_cmd_data1),
                NextValue(cmd_iso, self.i_cmd_iso),
                If (self.i_cmd_setup,
                        NextState('SETUP')
                ).Elif (self.i_cmd_in,
                        NextState('IN')
                ).Elif (self.i_cmd_out,
                        NextState('OUT')
                ).Else (NextState('IDLE')))

        fsm.act('SETUP',
                txstate.i_pkt_start.eq(1),
                txstate.i_pid.eq(PID.SETUP),
                If (txstate.o_pkt_end,
                    NextState('SEND_DATA')))

        fsm.act('IN',
                txstate.i_pkt_start.eq(1),
                txstate.i_pid.eq(PID.IN),
                If (txstate.o_pkt_end,
                    NextValue(reply_timeout, 0),
                    NextState('WAIT_REPLY')))

        fsm.act('OUT',
                txstate.i_pkt_start.eq(1),
                txstate.i_pid.eq(PID.OUT),
                If (txstate.o_pkt_end,
                    NextState('SEND_DATA')))

        fsm.act('SEND_DATA',
                txstate.i_pkt_start.eq(1),
                txstate.i_pid.eq(Mux(cmd_data1, PID.DATA1, PID.DATA0)),
                self.data_send_get.eq(txstate.o_data_ack),
                self.data_end.eq(txstate.o_pkt_end),
                If (txstate.o_pkt_end,
                    If(cmd_iso,
                       NextState('IDLE')
                    ).Else(NextValue(reply_timeout, 0),
                           NextState('WAIT_REPLY'))))

        fsm.act('WAIT_REPLY',
                NextValue(reply_timeout, reply_timeout + 1),
                If (rxstate.o_decoded,
                    If ((rxstate.o_pid & PIDTypes.TYPE_MASK) == PIDTypes.DATA,
                        # restart the counter: it now bounds the data packet
                        NextValue(reply_timeout, 0),
                        NextState('RECV_DATA')
                    ).Else (NextState('IDLE'),
                            self.o_got_ack.eq(rxstate.o_pid == PID.ACK),
                            self.o_got_nak.eq(rxstate.o_pid == PID.NAK),
                            self.o_got_stall.eq(rxstate.o_pid == PID.STALL))
                ).Elif (reply_timeout >= reply_limit,
                        self.o_timeout.eq(1),
                        NextState('IDLE')
                ).Elif(self.i_cmd_setup | self.i_cmd_in | self.i_cmd_out,
                       NextValue(low_speed_override, 0),
                       If (self.i_cmd_pre,
                           NextState('PREAMBLE')
                       ).Else (NextState('START_TRANSFER'))))

        fsm.act('RECV_DATA',
                NextValue(reply_timeout, reply_timeout + 1),
                If(rx.o_pkt_end,
                   If (True,
                      NextState('END_DATA_LS')
                   ).Else (NextState('END_DATA'))
                # Escape hatch.  Without this a truncated inbound packet parks
                # the FSM here forever: it emits no event, so software sees a
                # transfer that never completes, and it never returns to IDLE,
                # so no later command can start either -- the same failure mode
                # WAIT_REPLY had before it got a watchdog.  Report it as a
                # timeout, which is what it looks like from software anyway.
                ).Elif (reply_timeout >= data_limit,
                        self.o_timeout.eq(1),
                        NextState('IDLE')))

        fsm.delayed_enter('END_DATA_LS', 'END_DATA', 8)

        fsm.act('END_DATA',
                # FIXME: Discard if CRC16 is incorrect
                self.o_got_data0.eq(rxstate.o_pid == PID.DATA0),
                self.o_got_data1.eq(rxstate.o_pid == PID.DATA1),
                If (cmd_iso,
                    NextState('IDLE')
                ).Else (NextState('ACK')))

        fsm.act('ACK',
                txstate.i_pkt_start.eq(1),
                txstate.i_pid.eq(PID.ACK),
                If (txstate.o_pkt_end,
                    NextState('IDLE')))

        if low_speed_support:
            fsm.act('KA',
                    NextValue(tx.i_keepalive, 1),
                    NextState('KA_WAIT1'))
            fsm.delayed_enter('KA_WAIT1', 'KA_MID', 10)
            fsm.act('KA_MID',
                    NextValue(tx.i_keepalive, 0),
                    NextState('KA_WAIT2'))
            fsm.delayed_enter('KA_WAIT2', 'KA_END', 10)
            fsm.act('KA_END',
                    NextValue(sof_latch, 0),
                    NextState('IDLE'))

        self.comb += [
            recv_strobe.eq(fsm.ongoing('RECV_DATA') & rx.o_data_strobe),
            self.data_recv_put.eq(recv_strobe & (recv_cnt == 2)),
            self.data_recv_payload.eq(recv_b0),
        ]
        self.sync.usb_12 += [
            If (~fsm.ongoing('RECV_DATA'),
                recv_cnt.eq(0)
            ).Elif (recv_strobe & (recv_cnt != 2),
                    recv_cnt.eq(recv_cnt + 1)),
            If (recv_strobe,
                recv_b0.eq(recv_b1),
                recv_b1.eq(rx.o_data_payload)),
        ]

        self.comb += [
            txstate.i_data_payload.eq(self.data_send_payload),
            txstate.i_data_ready.eq(self.data_send_have),
        ]
