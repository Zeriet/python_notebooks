package com.ge.current.em.entities.analytics;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * Created by 212554696 on 12/16/16.
 * initial version, needs review
 */
@Entity
@Table(name = "rule")
public class Rule implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "rule_id")
    private Integer ruleId;

    @Basic(optional = false)
    @Column(name = "rule_uid")
    private String ruleUid;

    @Column(name = "rule_name")
    private String ruleName;

    @Basic(optional = false)
    @Column(name = "rule_type")
    private String ruleType;

    @Basic(optional = false)
    @Column(name = "document_uid")
    private String documentUid;

    @Column(name = "source_rule_id")
    private Integer sourceRuleId;

    @Basic(optional = false)
    @Column(name = "from_date")
    @Temporal(TemporalType.DATE)
    private Date fromDate;

    @Basic(optional = false)
    @Column(name = "thru_date")
    @Temporal(TemporalType.DATE)
    private Date thruDate;

    @Column(name = "description")
    private String description;

    @Column(name = "authored_by")
    private String authoredBy;

    @Basic(optional = false)
    @Column(name = "update_timestamp")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updateTimestamp;

    @Basic(optional = false)
    @Column(name = "entry_timestamp")
    @Temporal(TemporalType.TIMESTAMP)
    private Date entryTimestamp;

    // TODO check constraints

    public Rule() {}

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Integer getRuleId() {
        return ruleId;
    }

    public void setRuleId(Integer ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleUid() {
        return ruleUid;
    }

    public void setRuleUid(String ruleUid) {
        this.ruleUid = ruleUid;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getRuleType() {
        return ruleType;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    public String getDocumentUid() {
        return documentUid;
    }

    public void setDocumentUid(String documentUid) {
        this.documentUid = documentUid;
    }

    public Integer getSourceRuleId() {
        return sourceRuleId;
    }

    public void setSourceRuleId(Integer sourceRuleId) {
        this.sourceRuleId = sourceRuleId;
    }

    public Date getFromDate() {
        return fromDate;
    }

    public void setFromDate(Date fromDate) {
        this.fromDate = fromDate;
    }

    public Date getThruDate() {
        return thruDate;
    }

    public void setThruDate(Date thruDate) {
        this.thruDate = thruDate;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAuthoredBy() {
        return authoredBy;
    }

    public void setAuthoredBy(String authoredBy) {
        this.authoredBy = authoredBy;
    }

    public Date getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(Date updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    public Date getEntryTimestamp() {
        return entryTimestamp;
    }

    public void setEntryTimestamp(Date entryTimestamp) {
        this.entryTimestamp = entryTimestamp;
    }

    @Override public String toString() {
        return "Rule{" +
                "ruleId=" + ruleId +
                ", ruleUid='" + ruleUid + '\'' +
                ", ruleName='" + ruleName + '\'' +
                ", ruleType='" + ruleType + '\'' +
                ", documentUid='" + documentUid + '\'' +
                ", sourceRuleId=" + sourceRuleId +
                ", fromDate=" + fromDate +
                ", thruDate=" + thruDate +
                ", description='" + description + '\'' +
                ", authoredBy='" + authoredBy + '\'' +
                ", updateTimestamp=" + updateTimestamp +
                ", entryTimestamp=" + entryTimestamp +
                '}';
    }
}
