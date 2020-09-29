package hadoop.crossProduct;

import org.apache.hadoop.io.Text;
import org.junit.Test;


import static org.junit.Assert.*;

public class QAScopeMapperTest {

    @Test
    public void get_group_key_of_qa_scope_record() {
        Text qaScopeText = new Text("IHawk AERO 2x2 v19-AI_V5,AeroScope,AS-001,,LOT01,38,2,2020-07-21 18:03:32.203,2,1,1,1,30,40.194088,41.142086,9.918829,47.340164,48.316338");
        BounderRecord record = BounderRecord.createQAScopeRecord(qaScopeText);

        assertEquals(record.getGroupKey(), new Text("LOT01221130"));
        assertEquals(record.getValueWithoutGroupKey(),
                "IHawk AERO 2x2 v19-AI_V5,AeroScope,AS-001,,38,2020-07-21 18:03:32.203,40.194088,41.142086,9.918829,47.340164,48.316338");
    }

    @Test
    public void get_group_key_of_qa_gauge_record() {
        Text qaGaugeText = new Text("IHawk AERO 2x2 v19-AI_V5,AutoGauge,AG-001,,LOT01,38,2,03:32.2,2,1,1,1,30,19.75723,8.413795");
        BounderRecord record = BounderRecord.createQAGaugeRecord(qaGaugeText);

        assertEquals(record.getGroupKey(), new Text("LOT01221130"));
        assertEquals(record.getValueWithoutGroupKey(),
                "IHawk AERO 2x2 v19-AI_V5,AutoGauge,AG-001,,38,03:32.2,19.75723,8.413795");

    }

    @Test
    public void get_group_key_of_motion_feature_first_bound() {
        Text mfbbText = new Text("IHawk AERO 2x2 v19-AI_V5,AeroBonder,IA17-070,BB,LOT01,38,2,03:32.2,2,1,1,1,30,0,Pass,0,149.887,199.945,99.983,-4902.969,12537.516,-12.377,4293.052,220,0,2896,8.125,1,1.125,30.6,396,376,5.634,16.188,382.571,50.233,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,11.549,3.438,7.063,1.063,1.563,15.269,0.124,14.228,-189.585,107.954,97.507,0.063,81.281,0,0,1.438,11.401,7.375,8.722,99.922,3,9,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0.625,10.652,19.93,-2.111,0.241,-1.49,0.438,2.875,15.246,2.983,0,0,0,0,0,0,0,0,0,1,1.25,0,1.44,727.273,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0.063,10.244,15.341,-0.373,0.241,0.073,-1.052,9.625,11.958,0.755,1,1.563,11.746,8.5,9.501,179.748,3.188,3,1.563,1,1.25,1.25,2.06,727.273,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-0.073,0.25");
        BounderRecord record = BounderRecord.createMotionFeatureFirstBoundRecord(mfbbText);


        assertEquals(record.getGroupKey(), new Text("LOT01221130"));
        assertEquals(record.getValueWithoutGroupKey(),
                "IHawk AERO 2x2 v19-AI_V5,AeroBonder,IA17-070,BB,38,03:32.2,0,Pass,0,149.887,199.945,99.983,-4902.969,12537.516,-12.377,4293.052,220,0,2896,8.125,1.125,30.6,396,376,5.634,16.188,382.571,50.233,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,11.549,3.438,7.063,1.063,1.563,15.269,0.124,14.228,-189.585,107.954,97.507,0.063,81.281,0,0,1.438,11.401,7.375,8.722,99.922,3,9,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.625,10.652,19.93,-2.111,0.241,-1.49,0.438,2.875,15.246,2.983,0,0,0,0,0,0,0,0,0,1.25,0,1.44,727.273,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.063,10.244,15.341,-0.373,0.241,0.073,-1.052,9.625,11.958,0.755,1.563,11.746,8.5,9.501,179.748,3.188,3,1.563,1.25,1.25,2.06,727.273,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-0.073,0.25");
    }

    @Test
    public void get_group_key_of_motion_feature_second_bound() {
        Text mfbsText = new Text("IHawk AERO 2x2 v19-AI_V5,AeroBonder,IA17-070,BS,LOT01,38,2,2020-07-21 18:03:32.203,2,1,1,2,30,0,Pass,0,149.887,199.945,99.983,-709.688,11617.359,-12.377,4293.052,9.621,2.938,3.563,1.063,1.938,9.953,0.465,10.276,-220.945,118.304,109.034,0,78.367,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0.75,64.848,74.196,0,2.9,2.461,8.408,2.25,69.398,5.994,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,11.125,35.057,38.317,-1.227,1.169,-1.19,8.722,0.75,37.184,4.099,1,1.5,12.096,11.188,9.115,149.956,3.188,2,1.875,0,0,0,0,0,0,0,0,0,0,1,0,0,0.11,3.218,3.016,13,1,2.188,4.738,10.959,-1.783,0.387,-1.154,6.75,2.063,7.948,5.284,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2.073,4.688,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-0.044,0.313,0,0,0,240.861,6.625,0,0,0,0,0,0,0,0,0,0,0");
        BounderRecord record = BounderRecord.createMotionFeatureSecondBoundRecord(mfbsText);

        assertEquals(record.getGroupKey(), new Text("LOT01221130"));
        assertEquals(record.getValueWithoutGroupKey(),
                "IHawk AERO 2x2 v19-AI_V5,AeroBonder,IA17-070,BS,38,2020-07-21 18:03:32.203,0,Pass,0,149.887,199.945,99.983,-709.688,11617.359,-12.377,4293.052,9.621,2.938,3.563,1.063,1.938,9.953,0.465,10.276,-220.945,118.304,109.034,0,78.367,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.75,64.848,74.196,0,2.9,2.461,8.408,2.25,69.398,5.994,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,11.125,35.057,38.317,-1.227,1.169,-1.19,8.722,0.75,37.184,4.099,1.5,12.096,11.188,9.115,149.956,3.188,1.875,0,0,0,0,0,0,0,0,0,0,0,0,0.11,3.218,3.016,13,2.188,4.738,10.959,-1.783,0.387,-1.154,6.75,2.063,7.948,5.284,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2.073,4.688,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,-0.044,0.313,0,0,0,240.861,6.625,0,0,0,0,0,0,0,0,0,0,0");

    }


}