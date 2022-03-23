package com.pixelengine.tools;
//2022-3-24 created


import com.pixelengine.DataModel.JRoi2;
import com.pixelengine.DataModel.WConfig;
import com.pixelengine.HBasePixelEngineHelper;

//读取二进制roi2数据
public class JRoi2Loader {
    public static byte[] loadData( String roi2IdOrRelFilepath , String pedir ){
        if( roi2IdOrRelFilepath.length()==0 ){
            System.out.println("JRoi2Loader.loadData error: null roi2IdOrRelFilepath");
            return null ;
        }else{
            byte[] roi2data = null ;
            if( roi2IdOrRelFilepath.contains("user:") ){
                JRoi2.Roi2ID roi2id= JRoi2.parseRoi2ID(roi2IdOrRelFilepath) ;
                if( roi2id==null ){
                    System.out.println("JRoi2Loader.loadData error:bad roi2id "+ roi2IdOrRelFilepath);
                    return null ;
                }
                HBasePixelEngineHelper hbaseHelper = new HBasePixelEngineHelper() ;
                roi2data = hbaseHelper.getRoiHsegTlv(1 , roi2id.id) ;
            }else if( roi2IdOrRelFilepath.contains("sys:") ){
                JRoi2.Roi2ID roi2id= JRoi2.parseRoi2ID(roi2IdOrRelFilepath) ;
                if( roi2id==null ){
                    System.out.println("JRoi2Loader.loadData error:bad roi2id "+ roi2IdOrRelFilepath);
                    return null ;
                }
                HBasePixelEngineHelper hbaseHelper = new HBasePixelEngineHelper() ;
                roi2data = hbaseHelper.getRoiHsegTlv(0 , roi2id.id) ;
            }else {
                //relative filepath of hseg.tlv file
                String absfilepath =  pedir + roi2IdOrRelFilepath;
                roi2data = FileDirTool.readFileAsBytes(absfilepath) ;
            }
            if( roi2data==null ){
                System.out.println("JRoi2Loader.loadData error:null roi2data for "+roi2IdOrRelFilepath);
                return null ;
            }
            return roi2data ;
        }
    }
}
