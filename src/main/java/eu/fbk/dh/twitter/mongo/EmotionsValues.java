package eu.fbk.dh.twitter.mongo;

import lombok.Data;

@Data
public class EmotionsValues {
    Integer nrcDict_Surprise;
    Double nrcVadDict_Valence;
    Integer nrcDict_Anger;
    Integer nrcDict_Joy;
    Integer nrcDict_Trust;
    Integer nrcDict_Fear;
    Double nrcVadDict_Arousal;
    Integer nrcPosNegDict_Positive;
    Integer nrcDict_Sadness;
    Integer nrcDict_Anticipation;
    Integer nrcDict_Disgust;
    Double nrcVadDict_Dominance;
    Integer nrcPosNegDict_Negative;
}
