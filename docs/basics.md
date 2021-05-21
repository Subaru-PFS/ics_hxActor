## Overview

The `hxhal` library directly connects to the SAM, and provides SAM and ASIC register reading and writing plus many of the routines in a closed-source Teledyne IDL library.

The `hxActor` wraps the `hxhal` library. Takes commands in over the network, generates FITS files. Fairly easy to add new commands.

### hxhal library commands

The `hxhal` library does completely encapsulate everything you *need* to control a SAM (and probably Jade), an ASIC, and an H2RG or an H4RG. It can control H2RGs over Ethernet or USB, but due to a design flaw on the SAM's Ethernet card can only control H4RGs over USB. All instrument-specific code is optional. For USB control, the product is pure-python, but does depend on a freely available library (binary) and kernel driver (source) for the QuickUsb device.

It was intended to _duplicate_ a couple of Teledyne IDL libraries. That was the right thing to do at the time, but the choice did freeze in a number of annoying idioms and naming conventions.


Basic operation is pretty simple. A sample program to connect, configure a bit, and read a ramp:
```python
In [36]: import sam

In [37]: daq = sam.SAM(linkType='usb', instrumentName='PFS', bouncePower=True, logLevel=30)
In [38]: daq.updateHxRgConfigParameters('h4rgConfig', 'warmNoIrp')

In [39]: daq.sampleVoltage('VReset')
Out[39]: (0.30058148203369617, 19564)
In [40]: daq.setBiasVoltage('VReset', 0.25)
Out[40]: 0.2482893450635386
In [41]: daq.sampleVoltage('VReset')
Out[41]: (0.2712837334128523, 19171)

In [42]: daq.link.ReadAsicReg(0x4010)
Out[42]: 4

In [44]: im = daq.takeRamp(nResets=1, nReads=2, noReturn=False, noFiles=True)
2021-05-14 19:19:46.802Z qusbReads    20 qusb.py:640 readImage((4096, 4096)) done in 5.68s, with 32 chunks
2021-05-14 19:19:52.329Z qusbReads    20 qusb.py:640 readImage((4096, 4096)) done in 5.48s, with 32 chunks

In [45]: im.shape
Out[45]: (2, 4096, 4096)

```

### Actor commands

The `hxActor` wraps the `hxhal` library, runs permanently, takes external commands, and generates PFS FITS files. It is very PFS-centric.

The simplest way to send commands is from the shell, with `oneCmd.py $actorname $command`, e.g. `oneCmd.py hx_n1 ramp nreads=2`, which requests a two-read ramp from the n1 cryostat. If you want to make calls from a python, etc. library, making an external shell call is probably the best bet.

The basic commands are:
- `readAsic reg=N [nreg=N]`: read some ASIC registers. `nreg` defaults to 1.
    e.g. `readAsic reg=0x4058 nreg=2` returns the two IRP registers.
- `writeAsic reg=N value=N`: write a single ASIC register.
- `reconfigAsic`: trigger the ASIC reconfiguration process. Usually done indrectly from the `hxconfig` command, which always sets all the known configuration registers.If you call `reconfigAsic` directly, it is up to you to make sure those make sense.
- `getVoltage name=S`: measure a single bias voltage. This queries the ADC until the readings stabilize, usually a few tenths of a second.
  For historical reasons, the names are what are used in the Teledyne IDL code, including case. Sorry. Run `getVoltages` to get a full listing.
- `getVoltages`: measures all the bias voltages. Too many: should drop the ones we do not care about.
- `getSpiRegisters`: reads all the SPI registers on the H4. Should be boring but not all 0s!
- `getAsicPower`: return the ASIC load as seen by the SAM. Bank 0 should be 200-250 mW, the rest just low and spurious values (20-50 mW, on VDDIO and "VDD").
- `ramp [nreset=N] [nread=N] [ngroup=N] [ndrop=N] [exptype=S] [objname=S] [lamp=N] [lampPower=N] [outputReset] [rawImage]`: take a single ramp.
  `ngroup` and `ndrop` are probably untested. I suspect they work.
  `exptype` should be `flat` or `dark`.
  `lamp` and `lampPower` depend on the test cryostat. For the n8 cryostat at IDG, `lamp` is 1..4, and `lampPower` is 0..1023. The turn on time is slightly suspect: it should happen immediately after the reset finishing, but that time is sloppy right now.
  `outputReset` is untested. I do not think it works, but need it to.
  `rawImage` will leave any IRP pixels in place: reads will not be split into `IMAGE_N` and `REF_N` HDUs.

Am adding two configuration commands now:
- `loadRegisters <filename>`, with `addr value` lines. Just quicker than doing that one-by-one.
- `configureAsic <filename>`, which wraps the too-hardwired `hxhal` configuration mechanism.

The output FITS file will have one or two HDUs per read. `IMAGE_$N` for all reads, and `REF_$N` if IRP is enabled. N is 1-based. The IRP frame is *not* interpolated: if you have anything other than a 1:1 science:ref interleaving the frame will be smaller and will need to be fleshed out. Note that the pixels come in read order, which is always  `--> <--` for pair of channels when using the variable IRP-enabled firmware.

If I ever enable reading the reset frame, that will be named `RESET_$N`.


