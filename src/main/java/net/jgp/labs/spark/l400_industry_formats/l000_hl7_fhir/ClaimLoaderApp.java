package net.jgp.labs.spark.l400_industry_formats.l000_hl7_fhir;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.Claim;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.Identifier.IdentifierUse;

public class ClaimLoaderApp implements Serializable {
	private static final long serialVersionUID = -4250231621481140743L;

	private final class ClaimPrepAndProcess implements ForeachFunction<Row> {
		private static final long serialVersionUID = -3680381094052434862L;

		@Override
		public void call(Row r) throws Exception {
			
			// Build the claim
			// ---------------
			
			// CLIENT_ID ....... 0
			// CLIENT_SUB_ID ... 1
			// CLAIM_SK ........ 2
			// CLAIM_NBR ....... 3
			// CLAIM_ADJ_CD,ADJ_FROM_CLAIM_SK,ADJ_FROM_CLAIM_NBR,ADJ_FROM_CLAIM_ADJ_NBR,CLAIM_STS_CD_SK,CLAIM_STS_CD,SYS_SUBSC_SK,SYS_SUBSC_ID,SYS_MBR_SK,SYS_MBR_ID,CLIENT_HIERARCHY_LVL1,CLIENT_HIERARCHY_LVL2,CLIENT_HIERARCHY_LVL3,CLIENT_HIERARCHY_LVL4,MBR_PCP_SK,MBR_PCP_NBR,PROV_SK,PROV_ID,PROV_TP_CD_SK,PROV_TP_CD,PAYEE_SK,PAYEE_ID,PAYEE_TIN,APPROVE_DAY_AMT,ACTUAL_DAY_AMT,CLAIM_BEG_SVC_DT,RECEIVE_DT,PAY_DT,DRG_CD_SK,DRG_CD,SUBMIT_DRG_CD_SK,SUBMIT_DRG_CD,BILL_CLASS_CD,FACILITY_TP_CD,HOSP_FREQ_CD,ADMIT_SRC_CD_SK,ADMIT_SRC_CD,ADMIT_TP_CD_SK,ADMIT_TP_CD,HOSP_ADMIT_DT,HOSP_ADMIT_HR,HOSP_DISCHARGE_DT,HOSP_DISCHARGE_HR,HOSP_DISCHARGE_STS_CD_SK,HOSP_DISCHARGE_STS_CD,ATTENDING_PROV_SK,ATTENDING_PROV_ID,PROV_ADDR_CD_SK,PROV_ADDR_CD,OOA_IND,EDI_REF_ID,CURRENT_IND,SENSTV_DRG_IND,CLIENT_SPECIFIC_TXT1,CLIENT_SPECIFIC_TXT2,CLIENT_SPECIFIC_TXT3,CLIENT_SPECIFIC_TXT4,CLIENT_SPECIFIC_TXT5,CLIENT_SPECIFIC_TXT6,CLIENT_SPECIFIC_TXT7,CLIENT_SPECIFIC_TXT8,CLIENT_SPECIFIC_TXT9,CLIENT_SPECIFIC_TXT10,REC_LOAD_DTTM,REC_LAST_UPD_DTTM,REC_DEL_IND,DRG_CODE_TP_CD,SUBMIT_DRG_CODE_TP_CD,QA_CURRENT_IND
			CodeableConcept cc = new CodeableConcept();
			cc.setUserData("value", r.getString(3)); // TODO check that this is how this valued is set here

			Identifier i = new Identifier();
			i.setUse(IdentifierUse.OFFICIAL);
			i.setType(cc);
			i.setSystem("Payer Specific	 Claim Number");
			i.setValue(r.getAs("CLAIM_NBR"));
			// TODO i.setAssigner(value);

			List<Identifier> identifiers = new ArrayList<>();
			identifiers.add(i);
			
			Claim c = new Claim();
			c.setIdentifier(identifiers);
			
			// Process/send the claim
			// ----------------------
			
			// TODO
		}
	}

	public static void main(String[] args) {
		ClaimLoaderApp app = new ClaimLoaderApp();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("For Each Claim").master("local").getOrCreate();

		String filename = "data/claims.csv";
		Dataset<Row> claimsDf = spark.read().format("csv").option("inferSchema", "true").option("header", "true")
				.load(filename);
		claimsDf.show();

		claimsDf.foreach(new ClaimPrepAndProcess());
	}
}
