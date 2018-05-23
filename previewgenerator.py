import argparse
from astropy.io import fits
from scipy.misc import imsave
import numpy
from os import path

def main(args):
    infile = args.filename
    outfile = path.join(args.outdir, path.basename(infile))
    outpng = path.splitext(outfile)[0] + '.png'
    hdulist = fits.open(infile)
    data = hdulist[0].data
    # some kind of array manip
    hdulist[0].data = data
    hdulist.writeto(outfile, overwrite=True)
    print(hdulist[0])
    print(hdulist[0].header)
    print(hdulist[0].data)
    imsave(outpng, data) # uh oh, deprecated check PIL or imageio.imwrite instead?
    print('e')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('outdir')
    args = parser.parse_args()
    main(args)



'''
https://hea-www.harvard.edu/RD/xpa/index.html

The example I showed you was to cause it to load a new file using the
tool 'xpaset':

xpaset -p ds9 file fits dark_dark_start_2RW.fits

But you can actually send the image data through XPA too, in case the
viewer does not have filesystem access to it:

cat flat_flat_test_03RW.fits | xpaset ds9 fits

It can work over the network so the clients can be running on other
machines but it requires a little setup, in the example I showed ds9 was
running on alakai and I was using SSH X forwarding to display on my
machine so everything was local to alakai. Getting XPA setup for our
network might be worth doing:

http://ds9.si.edu/doc/faq.html#XPA

Looks like there are some python XPA libraries specifically for DS9:

https://github.com/ericmandel/pyds9

...so we might be able to generate previews and push to clients in one step. 
'''



'''
D3 is the javascript data visualization library:

https://d3js.org/

...and bokeh is the python frontend to D3:

https://bokeh.pydata.org/en/latest

A lot of the examples on the bokeh website use pandas, which is a data
library for python. If you haven't used it yet you might find it
interesting, it's simple and very powerful. Think of it like numpy
arrays with metadata so you can label and type axes. A lot of packages
understand pandas so if you use a pandas dataframe to store your data
you can pass it off without any arguments. Bokeh, for example, will know
if an axis is a time-series and automatically format and scale the plot
accordingly.

https://pandas.pydata.org'''