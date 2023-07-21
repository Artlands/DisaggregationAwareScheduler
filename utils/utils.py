from scipy.ndimage import zoom


def interpolate(inp, new_len):
  return zoom(inp, new_len/len(inp), order=0, mode='nearest')