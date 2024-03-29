// LZ.OutWindow

package SevenZip.Compression.LZ;

import java.io.IOException;

public class OutWindow
{
	/**
	 * @uml.property  name="_buffer" multiplicity="(0 -1)" dimension="1"
	 */
	byte[] _buffer;
	/**
	 * @uml.property  name="_pos"
	 */
	int _pos;
	/**
	 * @uml.property  name="_windowSize"
	 */
	int _windowSize = 0;
	/**
	 * @uml.property  name="_streamPos"
	 */
	int _streamPos;
	/**
	 * @uml.property  name="_stream"
	 */
	java.io.OutputStream _stream;
	
	public void Create(int windowSize)
	{
		if (_buffer == null || _windowSize != windowSize)
			_buffer = new byte[windowSize];
		_windowSize = windowSize;
		_pos = 0;
		_streamPos = 0;
	}
	
	public void SetStream(java.io.OutputStream stream) throws IOException
	{
		ReleaseStream();
		_stream = stream;
	}
	
	public void ReleaseStream() throws IOException
	{
		Flush();
		_stream = null;
	}
	
	public void Init(boolean solid)
	{
		if (!solid)
		{
			_streamPos = 0;
			_pos = 0;
		}
	}
	
	public void Flush() throws IOException
	{
		int size = _pos - _streamPos;
		if (size == 0)
			return;
                _stream.write(_buffer, _streamPos, size);
		if (_pos >= _windowSize)
			_pos = 0;
		_streamPos = _pos;
	}
	
	public void CopyBlock(int distance, int len) throws IOException
	{
		int pos = _pos - distance - 1;
		if (pos < 0)
			pos += _windowSize;
		for (; len != 0; len--)
		{
			if (pos >= _windowSize)
				pos = 0;
			_buffer[_pos++] = _buffer[pos++];
			if (_pos >= _windowSize)
				Flush();
		}
	}
	
	public void PutByte(byte b) throws IOException
	{
		_buffer[_pos++] = b;
		if (_pos >= _windowSize)
			Flush();
	}
	
	public byte GetByte(int distance)
	{
		int pos = _pos - distance - 1;
		if (pos < 0)
			pos += _windowSize;
		return _buffer[pos];
	}
}
