function main()
{
	let ds = pe.Dataset("mod/ndvi",20200500000000);
	let ds2 = ds.clip2("sys:1",0) ;
	return ds2 ;
}