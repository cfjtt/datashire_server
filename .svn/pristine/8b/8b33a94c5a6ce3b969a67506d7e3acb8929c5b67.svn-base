package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;

/**
 * Data Mining Squid实体类
 *
 * @author bo.dang
 * @date 2014年5月13日
 */
@MultitableMapping(name = {"DS_SQUID"}, pk = "ID", desc = "")
public class DataMiningSquid extends DataSquid {

    {
        this.setType(DSObjectType.SQUID.value());
    }

    @ColumnMpping(name = "training_percentage", desc = "训练集所占比例", nullable = true, precision = 1, type = Types.DOUBLE, valueReg = "")
    private double training_percentage;
    @ColumnMpping(name = "versioning", desc = "是否保留历史版本", nullable = true, precision = 200, type = Types.INTEGER, valueReg = "")
    private int versioning;
    @ColumnMpping(name = "min_batch_fraction", desc = "每次迭代处理的最小数据量", nullable = true, precision = 200, type = Types.DOUBLE, valueReg = "")
    private double min_batch_fraction;
    @ColumnMpping(name = "iteration_number", desc = "迭代次数", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int iteration_number;
    @ColumnMpping(name = "step_size", desc = "步长", nullable = true, precision = 1, type = Types.DOUBLE, valueReg = "")
    private double step_size;
    @ColumnMpping(name = "smoothing_parameter", desc = "平滑参数", nullable = true, precision = 0, type = Types.DOUBLE, valueReg = "")
    private double smoothing_parameter;
    @ColumnMpping(name = "regularization", desc = "正则化", nullable = true, precision = 0, type = Types.DOUBLE, valueReg = "")
    private double regularization;
    @ColumnMpping(name = "k", desc = "目标聚类组的个数", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int k;
    @ColumnMpping(name = "parallel_runs", desc = "并行数", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int parallel_runs;
    @ColumnMpping(name = "initialization_mode", desc = "初始化方式", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int initialization_mode;
    @ColumnMpping(name = "implicit_preferences", desc = "是否使用隐式偏好", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int implicit_preferences;
    @ColumnMpping(name = "case_sensitive", desc = "是否区分大小写", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int case_sensitive;
    @ColumnMpping(name = "min_value", desc = "量化区间最小值", nullable = true, precision = 0, type = Types.FLOAT, valueReg = "")
    private double min_value;
    @ColumnMpping(name = "max_value", desc = "量化区间最大值", nullable = true, precision = 0, type = Types.FLOAT, valueReg = "")
    private double max_value;
    @ColumnMpping(name = "bucket_count", desc = "对连续值离散时，最多可分为指定数目的取值区间", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int bucket_count;
    @ColumnMpping(name = "seed", desc = "初始种子", nullable = true, precision = 0, type = Types.BIGINT, valueReg = "")
    private long seed;

    // add yi.zhou 2014-09-16 决策树squid（DecisionTree SquidModelBase）
    @ColumnMpping(name = "algorithm", desc = "算法", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int algorithm;
    @ColumnMpping(name = "max_depth", desc = "决策树的最大深度", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int max_depth;
    @ColumnMpping(name = "impurity", desc = "计算标准", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int impurity;
    @ColumnMpping(name = "max_bins", desc = "最大特征数", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int max_bins;
    @ColumnMpping(name = "categorical_squid", desc = "分类映射表", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int categorical_squid;
    @ColumnMpping(name = "MIN_SUPPORT", desc = "最小支持度", nullable = true, precision = 0, type = Types.FLOAT, valueReg = "")
    private double min_support;
    @ColumnMpping(name = "MIN_CONFIDENCE", desc = "最小可信度", nullable = true, precision = 0, type = Types.FLOAT, valueReg = "")
    private double min_confidence;

    @ColumnMpping(name = "max_integer_number", desc = "最大迭代次数", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int max_integer_number;
    @ColumnMpping(name = "aggregation_depth", desc = "聚合树最大深度", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int aggregation_depth;
    @ColumnMpping(name = "fit_intercept", desc = "截距项", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int fit_intercept;
    @ColumnMpping(name = "solver", desc = "求解算法", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int solver;
    @ColumnMpping(name = "standardization", desc = "标准化数据", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int standardization;
    @ColumnMpping(name = "tolerance", desc = "允许误差", nullable = true, precision = 1, type = Types.DOUBLE, valueReg = "")
    private double tolerance;
    @ColumnMpping(name = "tree_number", desc = "树的数量", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int tree_number;
    @ColumnMpping(name = "feature_subset_strategy", desc = "特征子集策略", nullable = true, precision = 1, type = Types.INTEGER, valueReg = "")
    private int feature_subset_strategy;
    @ColumnMpping(name = "min_info_gain", desc = "最小信息增益", nullable = true, precision = 1, type = Types.DOUBLE, valueReg = "")
    private double min_info_gain;
    @ColumnMpping(name = "subsampling_rate", desc = "采样率", nullable = true, precision = 1, type = Types.DOUBLE, valueReg = "")
    private double subsampling_rate;
    @ColumnMpping(name = "initialweights", desc = "初始权值", nullable = true, precision = 0, type = Types.LONGNVARCHAR, valueReg = "")
    private String initialweights;
    @ColumnMpping(name = "layers", desc = "网络层", nullable = true, precision = 0, type = Types.LONGNVARCHAR, valueReg = "")
    private String layers;
    @ColumnMpping(name = "feature_subset_scale", desc = "特征子集比例", nullable = true, precision = 0, type = Types.DOUBLE, valueReg = "")
    private double feature_subset_scale;
    @ColumnMpping(name = "method", desc = "", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int method;
    @ColumnMpping(name = "x_model_squid_id", desc = "x标准化模型", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int x_model_squid_id;
    @ColumnMpping(name = "y_model_squid_id", desc = "y标准化模型", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int y_model_squid_id;
    @ColumnMpping(name = "max_categories", desc = "最大类数", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int max_categories;
    @ColumnMpping(name = "elastic_net_param", desc = "弹性网参数", nullable = true, precision = 0, type = Types.DOUBLE, valueReg = "")
    private double elastic_net_param;
    @ColumnMpping(name = "min_instances_per_node", desc = "每个节点最小实例数", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int min_instances_per_node;
    @ColumnMpping(name = "family", desc = "标签分布族", nullable = true, precision = 0, type = Types.LONGNVARCHAR, valueReg = "")
    private int family;
    @ColumnMpping(name = "threshold", desc = "分类阈值", nullable = true, precision = 0, type = Types.LONGNVARCHAR, valueReg = "")
    private String threshold;
    @ColumnMpping(name = "model_type", desc = "模型类型", nullable = true, precision = 0, type = Types.INTEGER, valueReg = "")
    private int model_type;
    @ColumnMpping(name="minDivisibleClusterSize",desc = "可分聚类最小样本数",nullable = true,precision = 0,type = Types.DOUBLE,valueReg = "")
    private Double minDivisibleClusterSize;
    @ColumnMpping(name="init_Steps",desc = "初始步数",nullable = true,precision = 0,type = Types.INTEGER,valueReg = "")
    private int init_Steps;


    public String getThreshold() {
        return threshold;
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }

    public double getElastic_net_param() {
        return elastic_net_param;
    }

    public void setElastic_net_param(double elastic_net_param) {
        this.elastic_net_param = elastic_net_param;
    }

    public int getMin_instances_per_node() {
        return min_instances_per_node;
    }

    public void setMin_instances_per_node(int min_instances_per_node) {
        this.min_instances_per_node = min_instances_per_node;
    }

    public int getFamily() {
        return family;
    }

    public void setFamily(int family) {
        this.family = family;
    }

    public int getMax_categories() {
        return max_categories;
    }

    public void setMax_categories(int max_categories) {
        this.max_categories = max_categories;
    }

    public int getX_model_squid_id() {
        return x_model_squid_id;
    }

    public void setX_model_squid_id(int x_model_squid_id) {
        this.x_model_squid_id = x_model_squid_id;
    }

    public int getY_model_squid_id() {
        return y_model_squid_id;
    }

    public void setY_model_squid_id(int y_model_squid_id) {
        this.y_model_squid_id = y_model_squid_id;
    }

    public int getMethod() {
        return method;
    }

    public void setMethod(int method) {
        this.method = method;
    }

    public double getMin_support() {
        return min_support;
    }

    public void setMin_support(double min_support) {
        this.min_support = min_support;
    }

    public double getMin_confidence() {
        return min_confidence;
    }

    public void setMin_confidence(double min_confidence) {
        this.min_confidence = min_confidence;
    }

    public double getTraining_percentage() {
        return training_percentage;
    }

    public void setTraining_percentage(double training_percentage) {
        this.training_percentage = training_percentage;
    }

    public int getVersioning() {
        return versioning;
    }

    public void setVersioning(int versioning) {
        this.versioning = versioning;
    }

    public double getMin_batch_fraction() {
        return min_batch_fraction;
    }

    public void setMin_batch_fraction(double min_batch_fraction) {
        this.min_batch_fraction = min_batch_fraction;
    }

    public int getIteration_number() {
        return iteration_number;
    }

    public void setIteration_number(int iteration_number) {
        this.iteration_number = iteration_number;
    }

    public double getStep_size() {
        return step_size;
    }

    public void setStep_size(double step_size) {
        this.step_size = step_size;
    }

    public double getSmoothing_parameter() {
        return smoothing_parameter;
    }

    public void setSmoothing_parameter(double smoothing_parameter) {
        this.smoothing_parameter = smoothing_parameter;
    }

    public double getRegularization() {
        return regularization;
    }

    public void setRegularization(double regularization) {
        this.regularization = regularization;
    }

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public int getParallel_runs() {
        return parallel_runs;
    }

    public void setParallel_runs(int parallel_runs) {
        this.parallel_runs = parallel_runs;
    }

    public int getInitialization_mode() {
        return initialization_mode;
    }

    public void setInitialization_mode(int initialization_mode) {
        this.initialization_mode = initialization_mode;
    }

    public int getImplicit_preferences() {
        return implicit_preferences;
    }

    public void setImplicit_preferences(int implicit_preferences) {
        this.implicit_preferences = implicit_preferences;
    }

    public int getCase_sensitive() {
        return case_sensitive;
    }

    public void setCase_sensitive(int case_sensitive) {
        this.case_sensitive = case_sensitive;
    }

    public double getMin_value() {
        return min_value;
    }

    public void setMin_value(double min_value) {
        this.min_value = min_value;
    }

    public double getMax_value() {
        return max_value;
    }

    public void setMax_value(double max_value) {
        this.max_value = max_value;
    }

    public int getBucket_count() {
        return bucket_count;
    }

    public void setBucket_count(int bucket_count) {
        this.bucket_count = bucket_count;
    }

    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    public int getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(int algorithm) {
        this.algorithm = algorithm;
    }

    public int getMax_depth() {
        return max_depth;
    }

    public void setMax_depth(int max_depth) {
        this.max_depth = max_depth;
    }

    public int getImpurity() {
        return impurity;
    }

    public void setImpurity(int impurity) {
        this.impurity = impurity;
    }

    public int getMax_bins() {
        return max_bins;
    }

    public void setMax_bins(int max_bins) {
        this.max_bins = max_bins;
    }

    public int getCategorical_squid() {
        return categorical_squid;
    }

    public void setCategorical_squid(int categorical_squid) {
        this.categorical_squid = categorical_squid;
    }

    public int getMax_integer_number() {
        return max_integer_number;
    }

    public void setMax_integer_number(int max_integer_number) {
        this.max_integer_number = max_integer_number;
    }

    public int getAggregation_depth() {
        return aggregation_depth;
    }

    public void setAggregation_depth(int aggregation_depth) {
        this.aggregation_depth = aggregation_depth;
    }

    public int getFit_intercept() {
        return fit_intercept;
    }

    public void setFit_intercept(int fit_intercept) {
        this.fit_intercept = fit_intercept;
    }

    public int getSolver() {
        return solver;
    }

    public void setSolver(int solver) {
        this.solver = solver;
    }

    public int getStandardization() {
        return standardization;
    }

    public void setStandardization(int standardization) {
        this.standardization = standardization;
    }

    public double getTolerance() {
        return tolerance;
    }

    public void setTolerance(double tolerance) {
        this.tolerance = tolerance;
    }

    public int getTree_number() {
        return tree_number;
    }

    public void setTree_number(int tree_number) {
        this.tree_number = tree_number;
    }

    public int getFeature_subset_strategy() {
        return feature_subset_strategy;
    }

    public void setFeature_subset_strategy(int feature_subset_strategy) {
        this.feature_subset_strategy = feature_subset_strategy;
    }

    public double getMin_info_gain() {
        return min_info_gain;
    }

    public void setMin_info_gain(double min_info_gain) {
        this.min_info_gain = min_info_gain;
    }

    public double getSubsampling_rate() {
        return subsampling_rate;
    }

    public void setSubsampling_rate(double subsampling_rate) {
        this.subsampling_rate = subsampling_rate;
    }

    public String getInitialweights() {
        return initialweights;
    }

    public void setInitialweights(String initialweights) {
        this.initialweights = initialweights;
    }

    public String getLayers() {
        return layers;
    }

    public void setLayers(String layers) {
        this.layers = layers;
    }

    public double getFeature_subset_scale() {
        return feature_subset_scale;
    }

    public void setFeature_subset_scale(double feature_subset_scale) {
        this.feature_subset_scale = feature_subset_scale;
    }

    public int getModel_type() {
        return model_type;
    }

    public void setModel_type(int model_type) {
        this.model_type = model_type;
    }
    public Double getMinDivisibleClusterSize() {
        return minDivisibleClusterSize;
    }

    public void setMinDivisibleClusterSize(Double minDivisibleClusterSize) {
        this.minDivisibleClusterSize = minDivisibleClusterSize;
    }

    public int getInit_Steps() {
        return init_Steps;
    }

    public void setInit_Steps(int init_Steps) {
        this.init_Steps = init_Steps;
    }
}
